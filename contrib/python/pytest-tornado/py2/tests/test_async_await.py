import pytest
from tornado import gen

async def dummy_native_coroutine(io_loop):
    await gen.sleep(0)
    return True


@pytest.mark.gen_test
async def test_native_coroutine_gen_test(io_loop):
    result = await dummy_native_coroutine(io_loop)
    assert result


@pytest.mark.gen_test(run_sync=False)
async def test_native_coroutine_run_sync_false(io_loop):
    result = await dummy_native_coroutine(io_loop)
    assert result
