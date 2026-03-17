import asyncio
import datetime
import time

import psycopg2
import psycopg2.tz
import pytest

from aiopg import IsolationLevel, ReadCommittedCompiler
from aiopg.connection import TIMEOUT


@pytest.fixture
def connect(make_connection):
    async def go(**kwargs):
        conn = await make_connection(**kwargs)
        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS tbl")
            await cur.execute("CREATE TABLE tbl (id int, name varchar(255))")
            for i in [(1, "a"), (2, "b"), (3, "c")]:
                await cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
            await cur.execute("DROP TABLE IF EXISTS tbl2")
            await cur.execute(
                """CREATE TABLE tbl2
                                      (id int, name varchar(255))"""
            )
            await cur.execute("DROP FUNCTION IF EXISTS inc(val integer)")
            await cur.execute(
                """CREATE FUNCTION inc(val integer)
                                      RETURNS integer AS $$
                                      BEGIN
                                      RETURN val + 1;
                                      END; $$
                                      LANGUAGE PLPGSQL;"""
            )
        return conn

    return go


@pytest.fixture
def cursor(connect, loop):
    async def go():
        return await (await connect()).cursor()

    cur = loop.run_until_complete(go())
    yield cur
    cur.close()


async def test_description(cursor):
    async with cursor as cur:
        assert cur.description is None
        await cur.execute("SELECT * from tbl;")

        assert (
            len(cur.description) == 2
        ), "cursor.description describes too many columns"

        assert (
            len(cur.description[0]) == 7
        ), "cursor.description[x] tuples must have 7 elements"

        assert (
            cur.description[0][0].lower() == "id"
        ), "cursor.description[x][0] must return column name"

        assert (
            cur.description[1][0].lower() == "name"
        ), "cursor.description[x][0] must return column name"

        # Make sure self.description gets reset, cursor should be
        # set to None in case of none resulting queries like DDL
        await cur.execute("DROP TABLE IF EXISTS foobar;")
        assert cur.description is None


async def test_raw(cursor):
    assert cursor._impl is cursor.raw


async def test_close(cursor):
    cursor.close()
    assert cursor.closed
    with pytest.raises(psycopg2.InterfaceError):
        await cursor.execute("SELECT 1")


async def test_close_twice(connect):
    conn = await connect()
    cur = await conn.cursor()
    cur.close()
    cur.close()
    assert cur.closed
    with pytest.raises(psycopg2.InterfaceError):
        await cur.execute("SELECT 1")
    assert conn._waiter is None


async def test_connection(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert cur.connection is conn


async def test_name(cursor):
    assert cursor.name is None


async def test_scrollable(cursor):
    assert cursor.scrollable is None
    with pytest.raises(psycopg2.ProgrammingError):
        cursor.scrollable = True


async def test_withhold(cursor):
    assert not cursor.withhold
    with pytest.raises(psycopg2.ProgrammingError):
        cursor.withhold = True
    assert not cursor.withhold


async def test_execute(cursor):
    await cursor.execute("SELECT 1")
    ret = await cursor.fetchone()
    assert (1,) == ret


async def test_executemany(cursor):
    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.executemany("SELECT %s", ["1", "2"])


def test_mogrify(cursor):
    ret = cursor.mogrify("SELECT %s", ["1"])
    assert b"SELECT '1'" == ret


async def test_setinputsizes(cursor):
    await cursor.setinputsizes(10)


async def test_fetchmany(cursor):
    await cursor.execute("SELECT * from tbl;")
    ret = await cursor.fetchmany()
    assert [(1, "a")] == ret

    await cursor.execute("SELECT * from tbl;")
    ret = await cursor.fetchmany(2)
    assert [(1, "a"), (2, "b")] == ret


async def test_fetchall(cursor):
    await cursor.execute("SELECT * from tbl;")
    ret = await cursor.fetchall()
    assert [(1, "a"), (2, "b"), (3, "c")] == ret


async def test_scroll(cursor):
    await cursor.execute("SELECT * from tbl;")
    await cursor.scroll(1)
    ret = await cursor.fetchone()
    assert (2, "b") == ret


async def test_arraysize(cursor):
    assert 1 == cursor.arraysize

    cursor.arraysize = 10
    assert 10 == cursor.arraysize


async def test_itersize(cursor):
    assert 2000 == cursor.itersize

    cursor.itersize = 10
    assert 10 == cursor.itersize


async def test_rows(cursor):
    await cursor.execute("SELECT * from tbl")
    assert 3 == cursor.rowcount
    assert 0 == cursor.rownumber
    await cursor.fetchone()
    assert 1 == cursor.rownumber


async def test_query(cursor):
    await cursor.execute("SELECT 1")
    assert b"SELECT 1" == cursor.query


async def test_statusmessage(cursor):
    await cursor.execute("SELECT 1")
    assert "SELECT 1" == cursor.statusmessage


async def test_tzinfo_factory(cursor):
    assert datetime.timezone is cursor.tzinfo_factory

    cursor.tzinfo_factory = psycopg2.tz.LocalTimezone
    assert psycopg2.tz.LocalTimezone is cursor.tzinfo_factory


async def test_nextset(cursor):
    with pytest.raises(psycopg2.NotSupportedError):
        await cursor.nextset()


async def test_setoutputsize(cursor):
    await cursor.setoutputsize(4, 1)


async def test_copy_family(connect):
    conn = await connect()
    cur = await conn.cursor()

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_from("file", "table")

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_to("file", "table")

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_expert("sql", "table")


async def test_callproc(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.callproc("inc", [1])
    ret = await cur.fetchone()
    assert (2,) == ret

    cur.close()
    with pytest.raises(psycopg2.InterfaceError):
        await cur.callproc("inc", [1])
    assert conn._waiter is None


async def test_execute_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.execute("SELECT pg_sleep(1)")
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_execute_override_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor()
    assert TIMEOUT == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.execute("SELECT pg_sleep(1)", timeout=timeout)
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_callproc_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.callproc("pg_sleep", [1])
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_callproc_override_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor()
    assert TIMEOUT == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.callproc("pg_sleep", [1], timeout=timeout)
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_echo(connect):
    conn = await connect(echo=True)
    cur = await conn.cursor()
    assert cur.echo


async def test_echo_false(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert not cur.echo


async def test_isolation_level(connect):
    conn = await connect()
    cur = await conn.cursor(isolation_level=IsolationLevel.read_committed)
    assert isinstance(cur._transaction._isolation, ReadCommittedCompiler)


async def test_iter(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM tbl")
    rows = []
    async for r in cur:
        rows.append(r)

    data = [(1, "a"), (2, "b"), (3, "c")]
    for item, tst in zip(rows, data):
        assert item == tst


async def test_echo_callproc(connect):
    conn = await connect(echo=True)
    cur = await conn.cursor()

    # TODO: check log records
    await cur.callproc("inc", [1])
    ret = await cur.fetchone()
    assert (2,) == ret
    cur.close()
