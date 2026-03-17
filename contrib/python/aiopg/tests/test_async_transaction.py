import psycopg2
import pytest

from aiopg import IsolationLevel, Transaction


@pytest.fixture
def engine(make_connection, loop):
    async def start():
        engine = await make_connection()
        async with engine.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS tbl")
            await cur.execute("CREATE TABLE tbl (id int, name varchar(255))")
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
async def test_transaction(engine, isolation_level, deferrable):
    async with engine.cursor() as cur:
        async with Transaction(
            cur, isolation_level, readonly=False, deferrable=deferrable
        ):
            await cur.execute("insert into tbl values(1, 'data')")
            await cur.execute("select * from tbl where id = 1")
            row = await cur.fetchone()

            assert row[0] == 1
            assert row[1] == "data"


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
async def test_transaction_readonly_insert(
    engine, isolation_level, deferrable
):
    async with engine.cursor() as cur:
        async with Transaction(
            cur, isolation_level, readonly=True, deferrable=deferrable
        ):
            with pytest.raises(psycopg2.InternalError):
                await cur.execute("insert into tbl values(1, 'data')")


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
async def test_transaction_readonly(engine, isolation_level, deferrable):
    async with engine.cursor() as cur:
        async with Transaction(
            cur, isolation_level, readonly=True, deferrable=deferrable
        ):
            await cur.execute("select * from tbl where id = 22")
            row = await cur.fetchone()

            assert row[0] == 22
            assert row[1] == "read only"


async def test_transaction_rollback(engine):
    async with engine.cursor() as cur:
        with pytest.raises(psycopg2.DataError):
            async with Transaction(cur, IsolationLevel.read_committed):
                await cur.execute("insert into tbl values(1/0, 'no data')")

        await cur.execute("select * from tbl")
        row = await cur.fetchall()
        assert row == [
            (22, "read only"),
        ]


async def test_transaction_point(engine):
    async with engine.cursor() as cur:
        async with Transaction(cur, IsolationLevel.read_committed) as tr:
            await cur.execute("insert into tbl values(1, 'data')")

            with pytest.raises(psycopg2.DataError):
                async with tr.point():
                    await cur.execute("insert into tbl values(1/0, 'data')")

            async with tr.point():
                await cur.execute("insert into tbl values(2, 'data point')")

            await cur.execute("insert into tbl values(3, 'data')")

            await cur.execute("select * from tbl")
            row = await cur.fetchall()
            assert row == [
                (22, "read only"),
                (1, "data"),
                (2, "data point"),
                (3, "data"),
            ]


async def test_begin(engine):
    async with engine.cursor() as cur:
        async with cur.begin():
            await cur.execute("insert into tbl values(1, 'data')")

        async with cur.begin():
            await cur.execute("select * from tbl")
            row = await cur.fetchall()
            assert row == [
                (22, "read only"),
                (1, "data"),
            ]


async def test_begin_nested(engine):
    async with engine.cursor() as cur:
        async with cur.begin_nested():
            await cur.execute("insert into tbl values(1, 'data')")

            with pytest.raises(psycopg2.DataError):
                async with cur.begin_nested():
                    await cur.execute("insert into tbl values(1/0, 'no data')")

            async with cur.begin_nested():
                await cur.execute("insert into tbl values(2, 'data')")

        async with cur.begin_nested():
            await cur.execute("select * from tbl")
            row = await cur.fetchall()
            assert row == [
                (22, "read only"),
                (1, "data"),
                (2, "data"),
            ]


async def test_begin_nested_fail(engine):
    async with engine.cursor() as cur:
        with pytest.raises(psycopg2.DataError):
            async with cur.begin_nested():
                await cur.execute("insert into tbl values(1/0, 'data')")

        async with cur.begin_nested():
            await cur.execute("select * from tbl")
            row = await cur.fetchall()
            assert row == [(22, "read only")]
