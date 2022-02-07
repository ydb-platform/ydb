import asyncio

from ydb.aio.connection import Connection
import pytest
from ydb import _apis


@pytest.mark.asyncio
async def test_async_call(aio_connection):
    request = _apis.ydb_scheme.DescribePathRequest()
    request.path = ""
    stub = _apis.SchemeService.Stub
    response = await aio_connection(request, stub, _apis.SchemeService.DescribePath)
    assert response.operation.ready


@pytest.mark.asyncio
async def test_close(aio_connection: Connection):
    request = _apis.ydb_scheme.DescribePathRequest()
    request.path = ""
    stub = _apis.SchemeService.Stub

    async def run_and_catch():
        from ydb.issues import ConnectionLost

        try:
            await aio_connection(request, stub, _apis.SchemeService.DescribePath)
        except ConnectionLost:
            pass

    tasks = [run_and_catch() for _ in range(10)]

    tasks.append(aio_connection.close(0))
    with pytest.raises(asyncio.CancelledError):
        await asyncio.gather(
            *tasks,
        )


@pytest.mark.asyncio
async def test_callbacks(aio_connection: Connection):
    cleanup_callback_called = False
    result_wrapped = False
    wrapper_args = ("1", "3", 10, "239")

    request = _apis.ydb_scheme.DescribePathRequest()
    request.path = ""
    stub = _apis.SchemeService.Stub

    def cleanup(conn):
        assert conn is aio_connection
        nonlocal cleanup_callback_called
        cleanup_callback_called = True

    def wrap(state, resp, *args):
        assert args == wrapper_args
        nonlocal result_wrapped
        result_wrapped = True

    aio_connection.add_cleanup_callback(cleanup)

    await aio_connection(
        request,
        stub,
        _apis.SchemeService.DescribePath,
        wrap_result=wrap,
        wrap_args=wrapper_args,
    )

    await aio_connection.close()

    assert result_wrapped and cleanup_callback_called
