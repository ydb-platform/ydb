import asyncio

import psycopg2
import pytest
from psycopg2.extensions import parse_dsn
from sqlalchemy import Column, Integer, MetaData, String, Table

from aiopg.connection import TIMEOUT

sa = pytest.importorskip("aiopg.sa")  # noqa


meta = MetaData()
tbl = Table(
    "sa_tbl3",
    meta,
    Column("id", Integer, nullable=False, primary_key=True),
    Column("name", String(255)),
)


@pytest.fixture
def engine(make_engine, loop):
    async def start():
        engine = await make_engine()
        async with engine.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS sa_tbl3")
            await conn.execute(
                "CREATE TABLE sa_tbl3 " "(id serial, name varchar(255))"
            )
        return engine

    return loop.run_until_complete(start())


def test_dialect(engine):
    assert sa.engine._dialect is engine.dialect


def test_name(engine):
    assert "postgresql" == engine.name


def test_driver(engine):
    assert "psycopg2" == engine.driver


def test_dsn(engine, pg_params):
    params = pg_params.copy()
    params["password"] = "xxx"
    params["dbname"] = params.pop("database")
    params["port"] = str(params["port"])
    assert parse_dsn(engine.dsn) == params


def test_minsize(engine):
    assert 1 == engine.minsize


def test_maxsize(engine):
    assert 10 == engine.maxsize


def test_size(engine):
    assert 1 == engine.size


def test_freesize(engine):
    assert 1 == engine.freesize


async def test_make_engine_with_default_loop(make_engine, loop):
    asyncio.set_event_loop(loop)
    engine = await make_engine()
    engine.close()
    await engine.wait_closed()


def test_not_context_manager(engine):
    with pytest.raises(RuntimeError):
        with engine:
            pass


async def test_release_transacted(engine):
    conn = await engine.acquire()
    tr = await conn.begin()
    with pytest.warns(ResourceWarning, match="Invalid transaction status"):
        await engine.release(conn)
    del tr
    assert conn.closed


def test_timeout(engine):
    assert TIMEOUT == engine.timeout


async def test_timeout_override(make_engine):
    timeout = 1
    engine = await make_engine(timeout=timeout)
    assert timeout == engine.timeout
    conn = await engine.acquire()
    with pytest.raises(asyncio.TimeoutError):
        await conn.execute("SELECT pg_sleep(10)")

    engine.terminate()
    await engine.wait_closed()


async def test_cannot_acquire_after_closing(make_engine):
    engine = await make_engine()
    engine.close()

    with pytest.raises(RuntimeError):
        await engine.acquire()

    await engine.wait_closed()


async def test_wait_closed(make_engine):
    engine = await make_engine(minsize=10)

    c1 = await engine.acquire()
    c2 = await engine.acquire()
    assert 10 == engine.size
    assert 8 == engine.freesize

    ops = []

    async def do_release(conn):
        await asyncio.sleep(0)
        engine.release(conn)
        ops.append("release")

    async def wait_closed():
        await engine.wait_closed()
        ops.append("wait_closed")

    engine.close()
    await asyncio.gather(wait_closed(), do_release(c1), do_release(c2))
    assert ["release", "release", "wait_closed"] == ops
    assert 0 == engine.freesize


async def test_terminate_with_acquired_connections(make_engine):
    engine = await make_engine()
    conn = await engine.acquire()
    engine.terminate()
    await engine.wait_closed()

    assert conn.closed


async def test_release_after_connection_disconnected_before_select(
    tcp_proxy, unused_port, pg_params, make_engine
):
    server_port = pg_params["port"]
    proxy_port = unused_port()

    tcp_proxy = await tcp_proxy(proxy_port, server_port)
    engine = await make_engine(port=proxy_port)

    with pytest.raises((psycopg2.InterfaceError, psycopg2.OperationalError)):
        with pytest.warns(ResourceWarning, match="Invalid transaction status"):
            async with engine.acquire() as conn, conn.begin():
                await conn.execute("SELECT 1;")
                await tcp_proxy.disconnect()
                await conn.execute("SELECT 1;")

    assert engine.size == 0


async def test_release_after_connection_disconnected_before_begin(
    tcp_proxy, unused_port, pg_params, make_engine
):
    server_port = pg_params["port"]
    proxy_port = unused_port()

    tcp_proxy = await tcp_proxy(proxy_port, server_port)
    engine = await make_engine(port=proxy_port)

    with pytest.raises((psycopg2.InterfaceError, psycopg2.OperationalError)):
        with pytest.warns(ResourceWarning, match="Invalid transaction status"):
            async with engine.acquire() as conn:
                await conn.execute("SELECT 1;")
                await tcp_proxy.disconnect()
                async with conn.begin():
                    pytest.fail("Should not be here")

    assert engine.size == 0
