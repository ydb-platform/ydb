"""Per-node fault state and the fault hook that every RPC goes through."""
import asyncio
import copy
import contextvars
import dataclasses
import time
import typing
import uuid

import grpc

from ydb.apps.bridge_skipper.ydbd_mock.lib.events import EventLog

_RPC_ID = contextvars.ContextVar("ydbd_mock_rpc_id", default=None)


@dataclasses.dataclass
class NodeFaults:
    down: bool = False                      # listener closed, counts against pile readiness
    unavailable: bool = False               # client-visible only: abort with gRPC UNAVAILABLE
    silence: bool = False                   # never answer until cleared or the client cancels
    silence_max: typing.Optional[float] = None
    silence_methods: typing.Optional[typing.Set[str]] = None   # None = every method
    delay: float = 0.0
    delay_methods: typing.Optional[typing.Set[str]] = None   # None = every method
    blame_storage: typing.Set[str] = dataclasses.field(default_factory=set)
    blame_connectivity: typing.Set[str] = dataclasses.field(default_factory=set)
    self_check: typing.Optional[str] = None
    failpoints: typing.Dict[str, dict] = dataclasses.field(default_factory=dict)
    response_overrides: typing.Dict[str, dict] = dataclasses.field(default_factory=dict)
    rpc_overrides: typing.Dict[str, dict] = dataclasses.field(default_factory=dict)

    def is_clear(self) -> bool:
        return self == NodeFaults()

    def clear(self) -> None:
        self.down = False
        self.unavailable = False
        self.silence = False
        self.silence_max = None
        self.silence_methods = None
        self.delay = 0.0
        self.delay_methods = None
        self.blame_storage = set()
        self.blame_connectivity = set()
        self.self_check = None
        self.failpoints.clear()
        self.response_overrides.clear()
        self.rpc_overrides.clear()

    def to_json(self) -> dict:
        out = {}
        if self.down:
            out["down"] = True
        if self.unavailable:
            out["unavailable"] = True
        if self.silence:
            out["silence"] = True
            if self.silence_max is not None:
                out["silence_max"] = self.silence_max
            if self.silence_methods is not None:
                out["silence_methods"] = sorted(self.silence_methods)
        if self.delay:
            out["delay"] = self.delay
            if self.delay_methods is not None:
                out["delay_methods"] = sorted(self.delay_methods)
        if self.blame_storage:
            out["storage"] = sorted(self.blame_storage)
        if self.blame_connectivity:
            out["connectivity"] = sorted(self.blame_connectivity)
        if self.self_check is not None:
            out["self_check"] = self.self_check
        if self.failpoints:
            out["failpoints"] = copy.deepcopy(list(self.failpoints.values()))
        if self.response_overrides:
            out["response_overrides"] = copy.deepcopy(self.response_overrides)
        if self.rpc_overrides:
            out["rpc_overrides"] = copy.deepcopy(self.rpc_overrides)
        return out


def short_method(method: str) -> str:
    return method.rsplit("/", 1)[-1]


class NodeRuntime:
    """Runs RPCs of one node through the fault hook and records rpc.* events.

    Order (spec 7.2): rpc.start; `unavailable` aborts with UNAVAILABLE; `delay`; `silence` waits
    until cleared (optionally auto-cleared after `silence_max`) or until the client cancels;
    `unavailable` is checked again; the method runs; rpc.end.
    """

    def __init__(self, node, faults: NodeFaults, events: EventLog) -> None:
        self.node = node
        self.faults = faults
        self.events = events
        self._cleared = asyncio.Event()
        self._cleared.set()
        self._auto_clear: typing.Optional[asyncio.TimerHandle] = None
        self.in_flight = 0
        self.pending_update_replies: typing.Set["asyncio.Future"] = set()
        self._failpoint_gates: typing.Dict[str, asyncio.Event] = {}

    def track_update_reply(self, context) -> None:
        """Keep a disconnecting update's listener until gRPC has sent its response or cancelled it."""
        reply = asyncio.get_running_loop().create_future()
        self.pending_update_replies.add(reply)

        def replied(_context) -> None:
            self.pending_update_replies.discard(reply)
            if not reply.done():
                reply.set_result(None)

        context.add_done_callback(replied)

    # ----- control -----

    def set_silence(self, on: bool, max_seconds: typing.Optional[float] = None,
                    methods: typing.Optional[typing.Iterable[str]] = None) -> None:
        self._cancel_auto_clear()
        if on:
            self.faults.silence = True
            self.faults.silence_max = max_seconds
            self.faults.silence_methods = set(methods) if methods else None
            self._cleared.clear()
            if max_seconds is not None:
                loop = asyncio.get_running_loop()
                self._auto_clear = loop.call_later(max_seconds, self._expire_silence)
        else:
            self.faults.silence = False
            self.faults.silence_max = None
            self.faults.silence_methods = None
            self._cleared.set()

    def _expire_silence(self) -> None:
        self._auto_clear = None
        if self.faults.silence:
            self.set_silence(False)
            self.events.append("fault", node=self.node.label, op="silence", on=False, reason="silence_max")

    def _cancel_auto_clear(self) -> None:
        if self._auto_clear is not None:
            self._auto_clear.cancel()
            self._auto_clear = None

    def clear(self) -> None:
        self._cancel_auto_clear()
        for gate in self._failpoint_gates.values():
            gate.set()
        self._failpoint_gates.clear()
        self.faults.clear()
        self._cleared.set()

    @staticmethod
    def _failpoint_key(method: str, stage: str) -> str:
        if not isinstance(method, str) or not method.startswith("/") or method.count("/") != 2:
            raise ValueError("failpoint method must be a full gRPC path /service/Method")
        if stage not in ("after_snapshot", "after_acceptance", "after_commit"):
            raise ValueError("unknown failpoint stage %r" % stage)
        return "%s@%s" % (method, stage)

    def configure_failpoint(self, method: str, stage: str, action: str = "hold", once: bool = False) -> None:
        key = self._failpoint_key(method, stage)
        if action not in ("hold", "unavailable"):
            raise ValueError("failpoint action must be hold or unavailable")
        if not isinstance(once, bool):
            raise ValueError("failpoint once must be a boolean")
        previous = self._failpoint_gates.pop(key, None)
        if previous is not None:
            previous.set()
        self.faults.failpoints[key] = {"method": method, "stage": stage, "action": action, "hits": 0, "once": once}
        self._failpoint_gates[key] = asyncio.Event()

    def release_failpoint(self, method: str, stage: str) -> None:
        key = self._failpoint_key(method, stage)
        self.faults.failpoints.pop(key, None)
        gate = self._failpoint_gates.pop(key, None)
        if gate is not None:
            gate.set()

    def failpoints_json(self) -> list:
        return copy.deepcopy(list(self.faults.failpoints.values()))

    async def checkpoint(self, method: str, stage: str, context) -> None:
        """Pause at a service-owned boundary; results must be captured before calling this hook."""
        key = self._failpoint_key(method, stage)
        point = self.faults.failpoints.get(key)
        if point is None or (point["once"] and point["hits"]):
            return
        point["hits"] += 1
        self.events.append("failpoint.hit", node=self.node.label, method=method, stage=stage,
                           action=point["action"], hits=point["hits"], rpc_id=_RPC_ID.get())
        if point["action"] == "unavailable":
            await context.abort(grpc.StatusCode.UNAVAILABLE, "mock: failpoint %s" % stage)
        else:
            await self._failpoint_gates[key].wait()
            self.events.append("failpoint.released", node=self.node.label, method=method, stage=stage, rpc_id=_RPC_ID.get())

    def set_response_override(self, method: str, result: typing.Optional[dict]) -> None:
        if method not in ("GetClusterState", "SelfCheck", "FetchConfig", "ListEndpoints"):
            raise ValueError("response overrides support GetClusterState, SelfCheck, FetchConfig, ListEndpoints")
        if result is None:
            self.faults.response_overrides.pop(method, None)
        elif isinstance(result, dict):
            self.faults.response_overrides[method] = copy.deepcopy(result)
        else:
            raise ValueError("response override result must be an object or null")

    def response_override(self, method: str) -> typing.Optional[dict]:
        return copy.deepcopy(self.faults.response_overrides.get(method))

    def set_rpc_override(self, method: str, response: typing.Optional[dict], once: bool = False) -> None:
        if method not in ("GetClusterState", "SelfCheck", "FetchConfig", "ListEndpoints", "UpdateClusterState",
                          "ConvertToBridge", "AddPile", "RemovePile"):
            raise ValueError("unsupported RPC override method %r" % method)
        if not isinstance(once, bool):
            raise ValueError("RPC override once must be a boolean")
        if response is None:
            self.faults.rpc_overrides.pop(method, None)
        elif isinstance(response, dict):
            self.faults.rpc_overrides[method] = {"response": copy.deepcopy(response), "once": once}
        else:
            raise ValueError("RPC override response must be an object or null")

    def take_rpc_override(self, method: str) -> typing.Optional[dict]:
        entry = self.faults.rpc_overrides.get(method)
        if entry is None:
            return None
        if entry["once"]:
            self.faults.rpc_overrides.pop(method)
        self.events.append("rpc.override", node=self.node.label, method=method,
                           rpc_id=_RPC_ID.get(), once=entry["once"], noncommitting=True)
        return copy.deepcopy(entry["response"])

    # ----- the hook -----

    def _silences(self, method: str) -> bool:
        f = self.faults
        if not f.silence:
            return False
        return f.silence_methods is None or short_method(method) in f.silence_methods or method in f.silence_methods

    def _delays(self, method: str) -> bool:
        f = self.faults
        if f.delay <= 0:
            return False
        return f.delay_methods is None or short_method(method) in f.delay_methods or method in f.delay_methods

    async def run(
        self,
        method: str,
        context,
        call: typing.Callable[[], typing.Awaitable],
        summary: str = "",
        status_of: typing.Callable[[typing.Any], str] = lambda result: "OK",
    ):
        started = time.monotonic()
        node = self.node.label
        rpc_id = uuid.uuid4().hex
        rpc_token = _RPC_ID.set(rpc_id)
        self.events.append("rpc.start", node=node, method=method, summary=summary, rpc_id=rpc_id)
        self.in_flight += 1
        status = "ERROR"
        try:
            if self.faults.unavailable:
                status = "UNAVAILABLE"
                await context.abort(grpc.StatusCode.UNAVAILABLE, "mock: node unavailable")
            if self._delays(method):
                await asyncio.sleep(self.faults.delay)
            if self._silences(method):
                await self._wait_silence()
            if self.faults.unavailable:
                status = "UNAVAILABLE"
                await context.abort(grpc.StatusCode.UNAVAILABLE, "mock: node unavailable")
            result = await call()
            status = status_of(result)
            return result
        except asyncio.CancelledError:
            status = "CANCELLED"
            self.events.append("rpc.cancelled", node=node, method=method,
                               duration_ms=int((time.monotonic() - started) * 1000), rpc_id=rpc_id)
            raise
        finally:
            self.in_flight -= 1
            if status != "CANCELLED":
                self.events.append("rpc.end", node=node, method=method, status=status,
                                   duration_ms=int((time.monotonic() - started) * 1000), rpc_id=rpc_id)
            _RPC_ID.reset(rpc_token)

    async def _wait_silence(self) -> None:
        await self._cleared.wait()
