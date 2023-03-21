import asyncio
import time
import typing

from .._grpc.grpcwrapper.common_utils import IToProto, IGrpcWrapperAsyncIO


class StreamMock(IGrpcWrapperAsyncIO):
    from_server: asyncio.Queue
    from_client: asyncio.Queue
    _closed: bool

    def __init__(self):
        self.from_server = asyncio.Queue()
        self.from_client = asyncio.Queue()
        self._closed = False

    async def receive(self) -> typing.Any:
        if self._closed:
            raise Exception("read from closed StreamMock")

        item = await self.from_server.get()
        if item is None:
            raise StopAsyncIteration()
        if isinstance(item, Exception):
            raise item
        return item

    def write(self, wrap_message: IToProto):
        if self._closed:
            raise Exception("write to closed StreamMock")
        self.from_client.put_nowait(wrap_message)

    def close(self):
        if self._closed:
            return

        self._closed = True
        self.from_server.put_nowait(None)


class WaitConditionError(Exception):
    pass


async def wait_condition(
    f: typing.Callable[[], bool],
    timeout: typing.Optional[typing.Union[float, int]] = None,
):
    """
    timeout default is 1 second
    if timeout is 0 - only counter work. It userful if test need fast timeout for condition (without wait full timeout)
    """
    if timeout is None:
        timeout = 1

    minimal_loop_count_for_wait = 1000

    start = time.monotonic()
    counter = 0
    while (time.monotonic() - start < timeout) or counter < minimal_loop_count_for_wait:
        counter += 1
        if f():
            return
        await asyncio.sleep(0)

    raise WaitConditionError("Bad condition in test")


async def wait_for_fast(
    awaitable: typing.Awaitable,
    timeout: typing.Optional[typing.Union[float, int]] = None,
):
    fut = asyncio.ensure_future(awaitable)
    await wait_condition(lambda: fut.done(), timeout)
    return fut.result()
