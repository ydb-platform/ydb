import asyncio

import pytest
import ydb


@pytest.mark.asyncio
async def test_simple_acquire(driver):
    pool = ydb.aio.SessionPool(driver, 1)
    session = await pool.acquire()

    for _ in range(10):
        with pytest.raises(ydb.SessionPoolEmpty):
            await pool.acquire(timeout=0.1)
    assert session.initialized()

    await pool.release(session)
    await pool.acquire()
    assert session.initialized()
    await pool.stop(timeout=10)


@pytest.mark.asyncio
async def test_waiter_is_notified(driver):
    pool = ydb.aio.SessionPool(driver, 1)
    session = await pool.acquire()
    task = asyncio.ensure_future(pool.acquire())
    await pool.release(session)
    sess = await asyncio.wait_for(task, timeout=0.1)
    assert sess.initialized()
    await pool.stop()


@pytest.mark.asyncio
async def test_no_race_after_future_cancel(driver):
    pool = ydb.aio.SessionPool(driver, 1)
    s = await pool.acquire()
    waiter = asyncio.ensure_future(pool.acquire())
    waiter.cancel()
    await pool.release(s)
    s = await pool.acquire()
    assert s.initialized()
    await pool.stop()


@pytest.mark.asyncio
async def test_release_logic(driver):
    pool = ydb.aio.SessionPool(driver, 1)
    session = await pool.acquire()
    waiter = asyncio.ensure_future(pool.acquire())
    session.reset()
    await pool.release(session)
    res = await waiter
    assert res.initialized()
    await pool.stop()


@pytest.mark.asyncio
async def test_close_basic_logic_case_1(driver):
    pool = ydb.aio.SessionPool(driver, 1)
    s = await pool.acquire()
    waiter = asyncio.ensure_future(pool.acquire())

    await pool.stop()
    waiter_sess = waiter.result()
    assert not waiter_sess.initialized()
    after_stop = await pool.acquire()
    assert not after_stop.initialized()

    await pool.release(s)
    await pool.release(after_stop)
    await pool.release(waiter_sess)
    assert pool._active_count == 0


@pytest.mark.asyncio
async def test_no_cluster_endpoints_no_failure(driver, docker_project):
    pool = ydb.aio.SessionPool(driver, 1)
    docker_project.stop()
    waiter = asyncio.ensure_future(pool.acquire())
    docker_project.start()
    sess = await asyncio.wait_for(waiter, 10)
    assert sess.initialized()
    sess.reset()
    await pool.release(sess)
    assert pool._active_count == 0
    await pool.stop()


@pytest.mark.asyncio
async def test_close_basic_logic_case_2(driver):
    pool = ydb.aio.SessionPool(driver, 10)
    acquired = []

    for _ in range(10):
        acquired.append(await pool.acquire())

    for _ in range(3):
        await pool.release(acquired.pop(-1))

    await pool.stop()
    assert pool._active_count == 7

    while acquired:
        await pool.release(acquired.pop(-1))

    assert pool._active_count == 0

    sess = await pool.acquire()

    assert not sess.initialized()
    await pool.stop()


@pytest.mark.asyncio
async def test_min_size_feature(driver):
    pool = ydb.aio.SessionPool(driver, 10, min_pool_size=10)

    await pool.wait_until_min_size()

    assert pool._active_count == 10
    session = await pool.acquire()
    session.reset()
    await pool.release(session)
    await pool.wait_until_min_size()

    await pool.stop()


@pytest.mark.asyncio
async def test_no_session_leak(driver, docker_project):
    pool = ydb.aio.SessionPool(driver, 1)
    docker_project.stop()
    try:
        await pool.acquire(timeout=0.5)
    except ydb.Error:
        pass
    assert pool._active_count == 0

    docker_project.start()
    await pool.stop()
