from typing import Any, Dict, List, NamedTuple, Optional, TypeVar

from .helpers import CONSUMER, PRODUCER, Endpoint, TraceContext, filter_none
from .mypy_types import OptInt, OptStr


class Annotation(NamedTuple):
    value: str
    timestamp: int


def _endpoint_asdict(endpoint: Endpoint) -> Dict[str, Any]:
    return filter_none(endpoint._asdict())


T = TypeVar("T", bound="Record")


class Record:
    def __init__(self: T, context: TraceContext, local_endpoint: Endpoint) -> None:
        self._context = context
        self._local_endpoint = _endpoint_asdict(local_endpoint)
        self._finished = False

        self._name = "unknown"
        self._kind: OptStr = None
        self._timestamp: OptInt = None
        self._duration: OptInt = None
        self._remote_endpoint: Optional[Dict[str, Any]] = None
        self._annotations: List[Annotation] = []
        self._tags: Dict[str, str] = {}

    @property
    def context(self) -> TraceContext:
        return self._context

    def start(self: T, ts: int) -> T:
        self._timestamp = ts
        return self

    def finish(self: T, ts: OptInt) -> T:
        if self._finished:
            return self
        if self._timestamp is None:
            raise RuntimeError("Record should be started first")
        if ts is not None and self._kind not in (PRODUCER, CONSUMER):
            self._duration = max(ts - self._timestamp, 1)
        self._finished = True
        return self

    def name(self: T, n: str) -> T:
        self._name = n
        return self

    def set_tag(self: T, key: str, value: Any) -> T:
        self._tags[key] = str(value)
        return self

    def annotate(self: T, value: Optional[str], ts: int) -> T:
        self._annotations.append(Annotation(str(value), int(ts)))
        return self

    def kind(self: T, kind: str) -> T:
        self._kind = kind
        return self

    def remote_endpoint(self: T, endpoint: Endpoint) -> T:
        self._remote_endpoint = _endpoint_asdict(endpoint)
        return self

    def asdict(self) -> Dict[str, Any]:
        c = self._context
        rec = {
            "traceId": c.trace_id,
            "name": self._name,
            "parentId": c.parent_id,
            "id": c.span_id,
            "kind": self._kind,
            "timestamp": self._timestamp,
            "duration": self._duration,
            "debug": c.debug,
            "shared": c.shared,
            "localEndpoint": self._local_endpoint,
            "remoteEndpoint": self._remote_endpoint,
            "annotations": [a._asdict() for a in self._annotations],
            "tags": self._tags,
        }
        return filter_none(rec, ["kind"])
