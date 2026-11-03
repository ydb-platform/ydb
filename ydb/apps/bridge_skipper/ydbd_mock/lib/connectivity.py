"""Reachability tracker: who does this instance currently consider connected.

`connected(p)` as seen by instance `s`:
- for `p == s`: its own node majority is up;
- otherwise: the peer is not on the block list, it answered a ping within `timeout`, and
  its last answer said `ready: true`. A peer that has never answered counts as connected
  during a grace period of `timeout` after this tracker was created.

Only the server side matters here. Client-visible faults (silence, delay, unavailable)
never reach this class.
"""
import time
import typing


class Reachability:
    def __init__(
        self,
        self_index: int,
        num_piles: int,
        timeout: typing.Union[float, typing.Callable[[], float]],
        clock: typing.Callable[[], float] = time.monotonic,
        own_ready: typing.Callable[[], bool] = lambda: True,
    ) -> None:
        self.self_index = self_index
        self.num_piles = num_piles
        self._timeout = timeout
        self._clock = clock
        self._own_ready = own_ready
        self._started = clock()
        self.last_ok: typing.Dict[int, float] = {}
        self.last_ready: typing.Dict[int, bool] = {}
        self.last_generation: typing.Dict[int, int] = {}
        self.blocked: typing.Set[int] = set()
        self._reported: typing.Dict[int, bool] = {p: True for p in range(num_piles) if p != self_index}

    @property
    def timeout(self) -> float:
        """Read on every use, so a change of the timing at runtime applies to a running instance."""
        return float(self._timeout() if callable(self._timeout) else self._timeout)

    @timeout.setter
    def timeout(self, value: float) -> None:
        self._timeout = value

    def restart(self) -> None:
        """Forget what was heard, as at process start."""
        self._started = self._clock()
        self.last_ok.clear()
        self.last_ready.clear()
        self.last_generation.clear()
        self._reported = {p: True for p in range(self.num_piles) if p != self.self_index}

    def record_ok(self, peer: int, ready: bool, generation: int) -> None:
        self.last_ok[peer] = self._clock()
        self.last_ready[peer] = bool(ready)
        self.last_generation[peer] = generation

    def reachable(self, peer: int) -> bool:
        """Ping-level reachability, ignoring the peer's own readiness."""
        if peer == self.self_index:
            return True
        if peer in self.blocked:
            return False
        ok = self.last_ok.get(peer)
        now = self._clock()
        if ok is None:
            return now - self._started < self.timeout
        return now - ok <= self.timeout

    def connected(self, peer: int) -> bool:
        if peer == self.self_index:
            return bool(self._own_ready())
        if not self.reachable(peer):
            return False
        return self.last_ready.get(peer, True)

    def evaluate(self) -> typing.List[typing.Tuple[int, bool]]:
        """Return (peer, connected) for every peer whose state changed since the last call."""
        changes = []
        for peer in sorted(self._reported):
            now = self.connected(peer)
            if now != self._reported[peer]:
                self._reported[peer] = now
                changes.append((peer, now))
        return changes

    def to_json(self, names: typing.Sequence[str]) -> typing.List[dict]:
        out = []
        for peer in range(self.num_piles):
            if peer == self.self_index:
                continue
            out.append({
                "pile": names[peer],
                "reachable": self.reachable(peer),
                "ready": self.last_ready.get(peer),
                "generation": self.last_generation.get(peer),
                "blocked": peer in self.blocked,
                "connected": self.connected(peer),
            })
        return out
