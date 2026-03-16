import pytest
import sqlalchemy as sa

meta = sa.MetaData()
tbl = sa.Table(
    "sa_tbl5",
    meta,
    sa.Column("ID", sa.String, primary_key=True, key="id"),
    sa.Column("Name", sa.String(255), key="name"),
)


@pytest.fixture
def connect(make_sa_connection, loop):
    async def start():
        conn = await make_sa_connection()
        await conn.execute("DROP TABLE IF EXISTS sa_tbl5")
        await conn.execute(
            "CREATE TABLE sa_tbl5 ("
            '"ID" VARCHAR(255) NOT NULL, '
            '"Name" VARCHAR(255), '
            'PRIMARY KEY ("ID"))'
        )

        await conn.execute(tbl.insert().values(id="test1", name="test_name"))
        await conn.execute(tbl.insert().values(id="test2", name="test_name"))
        await conn.execute(tbl.insert().values(id="test3", name="test_name"))

        return conn

    return loop.run_until_complete(start())


async def test_insert(connect):
    await connect.execute(tbl.insert().values(id="test-4", name="test_name"))
    await connect.execute(tbl.insert().values(id="test-5", name="test_name"))
    assert 5 == len(await (await connect.execute(tbl.select())).fetchall())


async def test_two_cursor_create_context_manager(make_engine, connect):
    engine = await make_engine(maxsize=1)

    async with engine.acquire() as conn:
        r1 = await conn.execute(tbl.insert().values(id="1", name="test"))

        r2 = await conn.execute(tbl.select())
        await r2.fetchone()
        assert not r2.closed

        r3 = await conn.execute(tbl.insert().values(id="3", name="test"))

    assert r1.closed
    assert r2.closed
    assert r3.closed
