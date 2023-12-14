import asyncio
import concurrent.futures
import threading
import typing
from typing import Optional

from .. import operation, issues
from .._grpc.grpcwrapper.common_utils import IFromProtoWithProtoType

TimeoutType = typing.Union[int, float, None]


def wrap_operation(rpc_state, response_pb, driver=None):
    return operation.Operation(rpc_state, response_pb, driver)


ResultType = typing.TypeVar("ResultType", bound=IFromProtoWithProtoType)


def create_result_wrapper(
    result_type: typing.Type[ResultType],
) -> typing.Callable[[typing.Any, typing.Any, typing.Any], ResultType]:
    def wrapper(rpc_state, response_pb, driver=None):
        issues._process_response(response_pb.operation)
        msg = result_type.empty_proto_message()
        response_pb.operation.result.Unpack(msg)
        return result_type.from_proto(msg)

    return wrapper


_shared_event_loop_lock = threading.Lock()
_shared_event_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_shared_event_loop() -> asyncio.AbstractEventLoop:
    global _shared_event_loop

    if _shared_event_loop is not None:
        return _shared_event_loop

    with _shared_event_loop_lock:
        if _shared_event_loop is not None:
            return _shared_event_loop

        event_loop_set_done = concurrent.futures.Future()

        def start_event_loop():
            event_loop = asyncio.new_event_loop()
            event_loop_set_done.set_result(event_loop)
            asyncio.set_event_loop(event_loop)
            event_loop.run_forever()

        t = threading.Thread(
            target=start_event_loop,
            name="Common ydb topic event loop",
            daemon=True,
        )
        t.start()

        _shared_event_loop = event_loop_set_done.result()
        return _shared_event_loop


class CallFromSyncToAsync:
    _loop: asyncio.AbstractEventLoop

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def unsafe_call_with_future(self, coro: typing.Coroutine) -> concurrent.futures.Future:
        """
        returned result from coro may be lost
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def unsafe_call_with_result(self, coro: typing.Coroutine, timeout: TimeoutType):
        """
        returned result from coro may be lost by race future cancel by timeout and return value from coroutine
        """
        f = self.unsafe_call_with_future(coro)
        try:
            return f.result(timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError()
        finally:
            if not f.done():
                f.cancel()

    def safe_call_with_result(self, coro: typing.Coroutine, timeout: TimeoutType):
        """
        no lost returned value from coro, but may be slower especially timeout latency - it wait coroutine cancelation.
        """

        if timeout is not None and timeout <= 0:
            return self._safe_call_fast(coro)

        async def call_coro():
            task = self._loop.create_task(coro)
            try:
                res = await asyncio.wait_for(task, timeout)
                return res
            except asyncio.TimeoutError:
                try:
                    res = await task
                    return res
                except asyncio.CancelledError:
                    pass

                # return builtin TimeoutError instead of asyncio.TimeoutError
                raise TimeoutError()

        return asyncio.run_coroutine_threadsafe(call_coro(), self._loop).result()

    def _safe_call_fast(self, coro: typing.Coroutine):
        """
        no lost returned value from coro, but may be slower especially timeout latency - it wait coroutine cancelation.
        Wait coroutine result only one loop.
        """
        res = concurrent.futures.Future()

        async def call_coro():
            try:
                res.set_result(await coro)
            except asyncio.CancelledError:
                res.set_exception(TimeoutError())

        coro_future = asyncio.run_coroutine_threadsafe(call_coro(), self._loop)
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0), self._loop).result()
        coro_future.cancel()
        return res.result()

    def call_sync(self, callback: typing.Callable[[], typing.Any]) -> typing.Any:
        result = concurrent.futures.Future()

        def call_callback():
            try:
                res = callback()
                result.set_result(res)
            except BaseException as err:
                result.set_exception(err)

        self._loop.call_soon_threadsafe(call_callback)

        return result.result()
