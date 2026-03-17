import asyncio

import psycopg2
import pytest

from aiopg import IsolationLevel, Transaction


@pytest.fixture
def engine(make_engine, loop):
    async def start():
        engine = await make_engine()
        async with engine.acquire() as cur:
            await cur.execute("DROP TABLE IF EXISTS tbl")
            await cur.execute(
                "CREATE TABLE tbl (id int, " "name varchar(255))"
            )

            await cur.execute("insert into tbl values(22, 'read only')")
        return engine

    return loop.run_until_complete(start())


@pytest.mark.parametrize(
    "isolation_level",
    [
        IsolationLevel.default,
        IsolationLevel.read_committed,
        IsolationLevel.repeatable_read,
        IsolationLevel.serializable,
    ],
)
@pytest.mark.parametrize(
    "deferrable",
    [
        False,
        True,
    ],
)
async def test_transaction_oldstyle(engine, isolation_level, deferrable):
    async with engine.acquire() as cur:
        tr = Transaction(
            cur, isolation_level, readonly=False, deferrable=deferrable
        )
        await tr.begin()
        await cur.execute("insert into tbl values(1, 'data')")
        resp = await cur.execute("select * from tbl where id = 1")
        row = await resp.fetchone()

        assert row.id == 1
        assert row.name == "data"

        await tr.commit()


async def two_begin(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    try:
        await tr.begin()
    except psycopg2.ProgrammingError as e:
        await tr.rollback()
        raise e


async def two_commit(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    await tr.commit()
    await tr.commit()


async def two_rollback(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    await tr.rollback()
    await tr.rollback()


async def e_rollback_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.rollback_savepoint()


async def e_release_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.release_savepoint()


async def two_rollback_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    try:
        await tr.release_savepoint()
    except psycopg2.ProgrammingError as e:
        await tr.commit()
        raise e


async def e_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.savepoint()


async def e_commit_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    await tr.savepoint()
    try:
        await tr.savepoint()
    except psycopg2.ProgrammingError as e:
        await tr.rollback_savepoint()
        await tr.commit()
        raise e


@pytest.mark.parametrize(
    "fn",
    [
        two_begin,
        two_commit,
        two_rollback,
        e_rollback_savepoint,
        e_release_savepoint,
        e_savepoint,
        e_commit_savepoint,
        two_rollback_savepoint,
    ],
)
async def test_transaction_fail_oldstyle(engine, fn):
    with pytest.raises(psycopg2.ProgrammingError):
        async with engine.acquire() as cur:
            await fn(cur)


async def test_transaction_finalization_warning(engine, monkeypatch):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.read_committed)

        def valid(x, _):
            assert x in [
                "Invalid transaction status on released connection: 2",
                f"You have not closed transaction {tr!r}",
                f"You have not closed savepoint {tr!r}",
            ]

        monkeypatch.setattr("aiopg.warnings.warn", valid)
        await tr.begin()
        await tr.savepoint()


async def test_transaction_readonly_insert_oldstyle(engine):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.serializable, readonly=True)

        await tr.begin()
        with pytest.raises(psycopg2.InternalError):
            await cur.execute("insert into tbl values(1, 'data')")
        await tr.rollback()


async def test_transaction_readonly_oldstyle(engine):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.serializable, readonly=True)

        await tr.begin()
        resp = await cur.execute("select * from tbl where id = 22")
        row = await resp.fetchone()

        assert row.id == 22
        assert row.name == "read only"
        await tr.commit()


async def test_transaction_point_oldstyle(engine):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.read_committed)
        await tr.begin()

        await cur.execute("insert into tbl values(1, 'data')")

        try:
            await tr.savepoint()
            await cur.execute("insert into tbl values(1/0, 'no data')")
        except psycopg2.DataError:
            await tr.rollback_savepoint()

        await tr.savepoint()
        await cur.execute("insert into tbl values(2, 'data')")
        await tr.release_savepoint()

        await cur.execute("insert into tbl values(3, 'data')")

        resp = await cur.execute("select * from tbl")
        row = await resp.fetchall()
        assert row == [
            (22, "read only"),
            (1, "data"),
            (2, "data"),
            (3, "data"),
        ]

        await tr.commit()


async def test_timeout_in_transaction_context_manager(make_engine):
    engine = await make_engine(timeout=1)
    with pytest.raises(asyncio.TimeoutError):
        async with engine.acquire() as connection:
            async with Transaction(connection, IsolationLevel.read_committed):
                await connection.execute("SELECT pg_sleep(10)")

    engine.terminate()
    await engine.wait_closed()


async def test_timeout_in_savepoint_context_manager(make_engine):
    engine = await make_engine(timeout=1)
    with pytest.raises(asyncio.TimeoutError):
        async with engine.acquire() as connection:
            async with Transaction(
                connection, IsolationLevel.read_committed
            ) as transaction:
                async with transaction.point():
                    await connection.execute("SELECT pg_sleep(10)")

    engine.terminate()
    await engine.wait_closed()


async def test_cancel_in_transaction_context_manager(engine, loop):
    with pytest.raises(asyncio.CancelledError):
        async with engine.acquire() as connection:
            async with Transaction(connection, IsolationLevel.read_committed):
                task = loop.create_task(
                    connection.execute("SELECT pg_sleep(10)")
                )

                async def cancel_soon():
                    await asyncio.sleep(1)
                    task.cancel()

                loop.create_task(cancel_soon())
                await task


async def test_cancel_in_savepoint_context_manager(engine, loop):
    with pytest.raises(asyncio.CancelledError):
        async with engine.acquire() as connection:
            async with Transaction(
                connection, IsolationLevel.read_committed
            ) as transaction:
                async with transaction.point():
                    task = loop.create_task(
                        connection.execute("SELECT pg_sleep(10)")
                    )

                    async def cancel_soon():
                        await asyncio.sleep(1)
                        task.cancel()

                    loop.create_task(cancel_soon())
                    await task
