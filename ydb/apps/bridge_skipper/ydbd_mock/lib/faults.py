"""Per-node fault state and the fault hook that every RPC goes through."""
import asyncio
import dataclasses
import time
import typing

import grpc

from ydb.apps.bridge_skipper.ydbd_mock.lib.events import EventLog


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
        self.faults.clear()
        self._cleared.set()

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
        self.events.append("rpc.start", node=node, method=method, summary=summary)
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
                               duration_ms=int((time.monotonic() - started) * 1000))
            raise
        finally:
            self.in_flight -= 1
            if status != "CANCELLED":
                self.events.append("rpc.end", node=node, method=method, status=status,
                                   duration_ms=int((time.monotonic() - started) * 1000))

    async def _wait_silence(self) -> None:
        await self._cleared.wait()
