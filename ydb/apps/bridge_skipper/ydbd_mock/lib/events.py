"""Bounded in-memory event log of one instance, with an optional JSON-lines sink."""
import collections
import json
import time
import typing


def _last_seq(path: str) -> int:
    """`seq` of the last complete line of an existing JSON-lines sink, or 0."""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 65536))
            lines = f.read().splitlines()
    except OSError:
        return 0
    for raw in reversed(lines):
        try:
            return int(json.loads(raw)["seq"])
        except (ValueError, KeyError, TypeError):
            continue
    return 0


class EventLog:
    def __init__(
        self,
        pile: str,
        capacity: int = 10000,
        clock: typing.Callable[[], float] = time.time,
        sink_path: typing.Optional[str] = None,
    ) -> None:
        self.pile = pile
        self._clock = clock
        self._events: typing.Deque[dict] = collections.deque(maxlen=capacity)
        # a revived process appends to the log of the killed one: keep `seq` unique and growing across restarts
        self._seq = _last_seq(sink_path) if sink_path else 0
        self._sink = open(sink_path, "a", buffering=1) if sink_path else None

    @property
    def last_seq(self) -> int:
        return self._seq

    RESERVED = ("seq", "ts", "pile", "kind")

    def append(self, kind: str, **fields) -> dict:
        self._seq += 1
        event = {"seq": self._seq, "ts": round(self._clock(), 6), "pile": self.pile, "kind": kind}
        for key, value in fields.items():
            # a caller's field never replaces the common ones: `pile` is always the instance that logged it
            event["field_" + key if key in self.RESERVED else key] = value
        self._events.append(event)
        if self._sink is not None:
            try:
                self._sink.write(json.dumps(event, default=str) + "\n")
            except OSError:
                pass
        return event

    def query(
        self,
        since: int = 0,
        after_ts: typing.Optional[float] = None,
        kind: typing.Optional[str] = None,
        grep: typing.Optional[str] = None,
        limit: typing.Optional[int] = None,
    ) -> typing.List[dict]:
        """Events with seq > since, ts > after_ts, kind starting with `kind`, `grep` in the JSON text."""
        out = []
        for event in self._events:
            if event["seq"] <= since:
                continue
            if after_ts is not None and event["ts"] <= after_ts:
                continue
            if kind and not event["kind"].startswith(kind):
                continue
            if grep and grep not in json.dumps(event, default=str):
                continue
            out.append(event)
        if limit is not None and limit >= 0:
            out = out[:limit]
        return out

    def kinds(self) -> typing.List[str]:
        return [e["kind"] for e in self._events]

    def close(self) -> None:
        if self._sink is not None:
            self._sink.close()
            self._sink = None
