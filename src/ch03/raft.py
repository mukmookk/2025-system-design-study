"""
학습용으로 단순화한 Raft 구현.

RaftCluster를 통해 메모리 상에서 투표/로그 복제를 흉내내며,
스레드 없이 tick() 호출만으로 타임아웃과 하트비트 흐름을 보여준다.

시나리오별 메서드 실행 흐름 요약(단계의 의미 포함):
- 리더: tick() → heartbeat_elapsed 누적 → heartbeat_interval 도달 시 send_heartbeats() → 각 팔로워 replicate_to_follower(..., heartbeat_only=True) 호출
  * tick: 논리적 시간 한 틱 진행
  * heartbeat_elapsed/heartbeat_interval: 리더가 하트비트를 얼마나 보냈는지 추적, 주기 도달 시 전송
  * send_heartbeats: 새 로그 없이 AppendEntries를 보내 리더 살아있음을 알림
  * replicate_to_follower(..., heartbeat_only=True): 각 팔로워에 prev_log 정보만 담은 하트비트 RPC 전송
- 팔로워/후보: tick() → election_elapsed 누적 → election_timeout 도달 시 start_election() → term 증가/자기표/로그 최신성 포함 request_votes → 과반이면 become_leader(), 실패·더 큰 term이면 become_follower()
  * election_elapsed/election_timeout: 리더 하트비트가 안 온 시간을 누적, 랜덤 타임아웃 도달 시 선거 시작
  * start_election: 후보로 승격, term 1 증가, 자기에게 표, 타임아웃 재설정
  * request_votes: 각 노드에 내 마지막 로그 위치/term을 포함해 투표 요청, 최신 로그가 있는지 검증받음
  * become_leader: 과반 득표 시 리더로 전환, 복제 상태(next/match) 초기화
  * become_follower: 더 큰 term을 보면 즉시 팔로워로 내려감(낡은 후보/리더 방지)
- AppendEntries 수신: handle_append_entries() → term 검증/리더 갱신 → prev_log 충돌 검사 → 충돌 시 자르고 새 엔트리 추가 → leader_commit 따라 commit_index 조정 → apply_committed()
  * term 검증: 요청 term이 낮으면 거절, 높으면 팔로워 전환
  * prev_log 검사: 내 로그의 prev_log_index/term이 맞지 않으면 실패로 응답해 리더가 backtrack하도록 함
  * 충돌 처리/추가: prev_log 이후 부분에 term 충돌이 있으면 잘라내고 리더 엔트리를 붙임
  * commit_index 조정: 리더가 알려준 leader_commit까지 반영하되 내 로그 길이를 넘지 않도록 제한
  * apply_committed: commit_index까지 순서대로 상태 머신에 적용
- 리더의 클라이언트 명령: append_client_command() → 로그 추가 → 각 팔로워 replicate_to_follower() → 응답 handle_append_response()로 next/match 조정 → update_commit_index() → apply_committed()
  * append_client_command: 리더 로그에 (term, 명령) 추가
  * replicate_to_follower: prev_log/새 엔트리를 포함한 AppendEntries를 팔로워에 전송
  * handle_append_response: 성공 시 match/next 갱신, 실패 시 next_index 감소(backtrack)로 다시 맞추기
  * update_commit_index: 현재 term의 엔트리가 과반수에 복제됐는지 검사 후 commit_index 전진
  * apply_committed: 새 commit_index까지 상태 머신에 반영
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class LogEntry:
    term: int
    command: str


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int


class RaftNode:
    """리더/후보/팔로워 상태 전환과 로그 복제를 수행하는 단일 노드."""

    def __init__(self, node_id: str, peers: List[str], cluster: "RaftCluster") -> None:
        self.node_id = node_id
        self.peers = peers
        self.cluster = cluster

        self.state = "follower"  # 시작은 항상 follower
        self.current_term = 0
        self.voted_for: Optional[str] = None

        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0

        # 리더일 때 각 팔로워의 복제 진행 상황을 추적.
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self.election_elapsed = 0
        self.election_timeout = self._random_timeout()  # 각 노드마다 랜덤 선거 타이머
        self.heartbeat_interval = 2
        self.heartbeat_elapsed = 0

        self.applied_commands: List[str] = []  # 상태 머신에 반영된 명령 기록
        self.leader_id: Optional[str] = None

    def _random_timeout(self) -> int:
        # 노드마다 다른 랜덤 타임아웃으로 동시 선거 가능성을 낮춘다.
        return random.randint(5, 10)

    def tick(self) -> None:
        """시간을 한 번 전진시켜 리더는 하트비트, 팔로워는 선거 타임아웃을 처리한다."""
        if self.state == "leader":
            self.heartbeat_elapsed += 1
            if self.heartbeat_elapsed >= self.heartbeat_interval:
                self.heartbeat_elapsed = 0
                self.send_heartbeats()  # 주기적으로 heartbeat/AppendEntries 전송
            return

        self.election_elapsed += 1
        if self.election_elapsed >= self.election_timeout:
            self.start_election()  # 타임아웃 시 Candidate로 승격 후 투표 시작

    def start_election(self) -> None:
        """후보로 승격해 term을 올리고, 마지막 로그 정보와 함께 투표를 요청한다."""
        self.state = "candidate"  # 스스로를 후보로 전환
        self.current_term += 1  # 새로운 term 시작
        self.voted_for = self.node_id  # 자기 자신에게 1표 행사
        self.election_elapsed = 0  # 선거 타이머 리셋
        self.election_timeout = self._random_timeout()  # 다음 타임아웃 갱신
        votes = 1  # 자기 자신에게 1표 (시작 점수)

        # 마지막 로그 위치/term을 첨부해 다른 노드가 최신성 판단을 할 수 있게 한다
        last_index, last_term = self.last_log_info()
        responses = self.cluster.request_votes(
            candidate_id=self.node_id,
            term=self.current_term,
            last_log_index=last_index,
            last_log_term=last_term,
        )

        # 응답을 모아 과반을 계산하거나 더 큰 term을 만나면 즉시 철회
        for response in responses:
            if response.term > self.current_term:
                self.become_follower(response.term)  # 더 최신 term 발견 시 즉시 강등
                return
            if response.vote_granted:
                votes += 1

        if votes >= self.cluster.quorum_size:
            self.become_leader()  # 과반 확보 시 리더 취임

    def become_follower(self, term: int, leader_id: Optional[str] = None) -> None:
        """더 큰 term이나 새 리더를 보면 팔로워로 내려가고 타이머를 리셋한다."""
        self.state = "follower"
        self.current_term = term
        self.voted_for = None
        self.leader_id = leader_id
        self.election_elapsed = 0
        self.election_timeout = self._random_timeout()
        self.heartbeat_elapsed = 0

    def become_leader(self) -> None:
        """리더가 되면 각 팔로워의 next/match index를 초기화하고 즉시 하트비트를 보낸다."""
        self.state = "leader"
        self.leader_id = self.node_id
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
        self.match_index[self.node_id] = len(self.log)
        self.send_heartbeats()

    def last_log_info(self) -> (int, int):
        if not self.log:
            return 0, 0
        return len(self.log), self.log[-1].term

    def handle_request_vote(
        self,
        candidate_id: str,
        term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> RequestVoteResponse:
        """RequestVote 요청을 받아 term/로그 최신성/기표 여부를 검사하고 투표한다."""
        if term < self.current_term:
            return RequestVoteResponse(term=self.current_term, vote_granted=False)

        if term > self.current_term:
            self.become_follower(term)  # 더 큰 term을 보면 즉시 follower로 강등

        up_to_date = (last_log_term, last_log_index) >= self.last_log_info()
        already_voted = self.voted_for is None or self.voted_for == candidate_id

        if up_to_date and already_voted:
            self.voted_for = candidate_id
            self.election_elapsed = 0
            return RequestVoteResponse(term=self.current_term, vote_granted=True)

        return RequestVoteResponse(term=self.current_term, vote_granted=False)

    def handle_append_entries(
        self,
        leader_id: str,
        term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[LogEntry],
        leader_commit: int,
    ) -> AppendEntriesResponse:
        """AppendEntries(하트비트/로그 복제)를 받아 term, 충돌, 커밋 반영을 처리한다."""
        if term < self.current_term:
            return AppendEntriesResponse(
                term=self.current_term, success=False, match_index=self.commit_index
            )

        if term > self.current_term or self.state != "follower":
            self.become_follower(term, leader_id=leader_id)  # 새로운 리더 인지
        else:
            self.leader_id = leader_id

        self.election_elapsed = 0

        if prev_log_index > len(self.log):
            return AppendEntriesResponse(
                term=self.current_term, success=False, match_index=len(self.log)
            )

        if prev_log_index > 0 and self.log[prev_log_index - 1].term != prev_log_term:
            return AppendEntriesResponse(
                term=self.current_term, success=False, match_index=prev_log_index - 1
            )

        # 충돌이 있으면 잘라내고 새 엔트리를 순서대로 추가
        index = prev_log_index
        for entry in entries:
            index += 1
            if len(self.log) >= index:
                if self.log[index - 1].term != entry.term:
                    self.log = self.log[: index - 1]
                    self.log.append(entry)
                # term이 같다면 이미 동일 엔트리가 있으므로 유지
            else:
                self.log.append(entry)

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            self.apply_committed()

        return AppendEntriesResponse(
            term=self.current_term, success=True, match_index=index
        )

    def append_client_command(self, command: str) -> None:
        """리더가 클라이언트 명령을 로그에 추가하고 팔로워에 복제한다."""
        if self.state != "leader":
            raise RuntimeError(f"{self.node_id} 노드는 리더가 아닙니다")

        self.log.append(LogEntry(term=self.current_term, command=command))
        self.match_index[self.node_id] = len(self.log)

        for peer in self.peers:
            self.replicate_to_follower(peer)

        self.update_commit_index()
        self.apply_committed()

    def send_heartbeats(self) -> None:
        for peer in self.peers:
            self.replicate_to_follower(peer, heartbeat_only=True)

    def replicate_to_follower(self, follower_id: str, heartbeat_only: bool = False) -> None:
        """특정 팔로워에게 AppendEntries를 전송하고 응답으로 next/match를 갱신한다."""
        next_idx = self.next_index.get(follower_id, len(self.log) + 1)
        prev_index = max(0, next_idx - 1)
        prev_term = self.log[prev_index - 1].term if prev_index > 0 else 0

        entries: List[LogEntry] = []
        if not heartbeat_only and next_idx <= len(self.log):
            entries = self.log[next_idx - 1 :]

        response = self.cluster.send_append_entries(
            leader_id=self.node_id,
            follower_id=follower_id,
            term=self.current_term,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        self.handle_append_response(follower_id, response)

    def handle_append_response(
        self, follower_id: str, response: AppendEntriesResponse
    ) -> None:
        if response.term > self.current_term:
            self.become_follower(response.term)
            return

        if self.state != "leader":
            return

        if response.success:
            self.match_index[follower_id] = response.match_index
            self.next_index[follower_id] = response.match_index + 1
            self.update_commit_index()
            self.apply_committed()
        else:
            # 팔로워가 앞부분을 놓친 상태라면 next_index를 줄여 재시도
            next_idx = self.next_index.get(follower_id, len(self.log) + 1)
            self.next_index[follower_id] = max(1, next_idx - 1)  # 뒤로 물러나며 맞춰가기

    def update_commit_index(self) -> None:
        """현재 term의 엔트리가 과반수에 복제됐을 때 commit_index를 앞으로 움직인다."""
        if self.state != "leader":
            return

        for idx in range(len(self.log), self.commit_index, -1):
            replicated = sum(1 for match in self.match_index.values() if match >= idx)
            # 현재 term의 엔트리가 과반수에 복제되었을 때만 commit (Raft 안전성 규칙)
            if replicated >= self.cluster.quorum_size and self.log[idx - 1].term == self.current_term:
                self.commit_index = idx
                return

    def apply_committed(self) -> None:
        """commit_index까지의 엔트리를 순서대로 상태 머신(여기선 리스트)에 반영한다."""
        while self.last_applied < self.commit_index:
            entry = self.log[self.last_applied]
            self.applied_commands.append(entry.command)  # 단순 문자열 명령을 상태 머신에 반영
            self.last_applied += 1


class RaftCluster:
    """네트워크 없이 노드 간 RPC 호출을 전달하는 메모리 상의 클러스터 도우미."""

    def __init__(self, node_ids: List[str]) -> None:
        self.nodes: Dict[str, RaftNode] = {}
        for node_id in node_ids:
            peers = [pid for pid in node_ids if pid != node_id]
            self.nodes[node_id] = RaftNode(node_id=node_id, peers=peers, cluster=self)

    @property
    def quorum_size(self) -> int:
        return len(self.nodes) // 2 + 1

    def request_votes(
        self, candidate_id: str, term: int, last_log_index: int, last_log_term: int
    ) -> List[RequestVoteResponse]:
        responses: List[RequestVoteResponse] = []
        for node_id, node in self.nodes.items():
            if node_id == candidate_id:
                continue
            responses.append(
                node.handle_request_vote(
                    candidate_id=candidate_id,
                    term=term,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term,
                )
            )
        return responses

    def send_append_entries(
        self,
        leader_id: str,
        follower_id: str,
        term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[LogEntry],
        leader_commit: int,
    ) -> AppendEntriesResponse:
        follower = self.nodes[follower_id]
        return follower.handle_append_entries(
            leader_id=leader_id,
            term=term,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=leader_commit,
        )

    def tick_all(self) -> None:
        """모든 노드의 tick을 호출해 선거/하트비트 이벤트를 진행시킨다."""
        for node in self.nodes.values():
            node.tick()

    def leader(self) -> Optional[RaftNode]:
        for node in self.nodes.values():
            if node.state == "leader":
                return node
        return None


if __name__ == "__main__":
    # 리더 선출과 로그 복제를 간단히 시연
    random.seed(42)  # 재실행 시 동일한 타임아웃을 재현
    cluster = RaftCluster(["A", "B", "C"])

    # 일정 시간 tick을 진행해 리더가 나올 때까지 기다린다
    for _ in range(15):
        cluster.tick_all()
    leader = cluster.leader()
    print(f"선거 후 리더: {leader.node_id if leader else '없음'}")

    if leader:
        leader.append_client_command("set x=1")
        leader.append_client_command("set y=2")
        print("리더 로그:", [e.command for e in leader.log])
        print("팔로워 로그:")
        for node_id, node in cluster.nodes.items():
            if node_id == leader.node_id:
                continue
            print(node_id, [e.command for e in node.log])
        print("리더에서 커밋된 인덱스:", leader.commit_index, "상태 머신:", leader.applied_commands)
