import asyncio

import grpc
from grpc import aio
import pytest


async def _responses(call):
    async for response in call:
        yield response


class UnaryStreamInterceptor(aio.UnaryStreamClientInterceptor):
    async def intercept_unary_stream(self, continuation, details, request):
        return _responses(await continuation(details, request))


class StreamStreamInterceptor(aio.StreamStreamClientInterceptor):
    async def intercept_stream_stream(self, continuation, details, request_iterator):
        return _responses(await continuation(details, request_iterator))


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_stream", [False, True], ids=["unary-stream", "stream-stream"])
@pytest.mark.parametrize("interceptor_count", [1, 2])
@pytest.mark.parametrize("close_owner", [False, True], ids=["foreign-channel", "own-channel"])
async def test_close_channel_with_intercepted_stream(stream_stream, interceptor_count, close_owner):
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(request, context):
        started.set()
        await release.wait()
        yield b"reply"

    server = aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "test.Close",
                {
                    "Unary": grpc.unary_stream_rpc_method_handler(handler),
                    "Bidi": grpc.stream_stream_rpc_method_handler(handler),
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    interceptor = StreamStreamInterceptor if stream_stream else UnaryStreamInterceptor
    channel = aio.insecure_channel(f"127.0.0.1:{port}", interceptors=[interceptor() for _ in range(interceptor_count)])
    other_channel = aio.insecure_channel(f"127.0.0.1:{port}")
    call = None
    status_task = None
    try:
        if stream_stream:
            call = channel.stream_stream("/test.Close/Bidi")(timeout=5)
            # write() starts grpcio's status task on the response iterator wrapper.
            await call.write(b"request")
            status_task = call._status_code_task
        else:
            call = channel.unary_stream("/test.Close/Unary")(b"request", timeout=5)
            wrapped_call = await call._interceptors_task
            status_task = asyncio.create_task(wrapped_call.code())
            await asyncio.sleep(0)

        await asyncio.wait_for(started.wait(), 5)
        await (channel if close_owner else other_channel).close()

        if close_owner:
            assert await asyncio.wait_for(call.code(), 5) == grpc.StatusCode.CANCELLED
        else:
            release.set()
            assert [response async for response in call] == [b"reply"]
            assert await call.code() == grpc.StatusCode.OK
    finally:
        release.set()
        if call is not None:
            call.cancel()
            await asyncio.wait_for(call.code(), 5)
        if status_task is not None:
            await asyncio.wait_for(status_task, 5)
        await channel.close()
        await other_channel.close()
        await server.stop(None)
