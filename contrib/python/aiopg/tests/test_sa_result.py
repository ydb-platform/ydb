import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

meta = MetaData()
tbl = Table(
    "sa_tbl",
    meta,
    Column("id", Integer, nullable=False, primary_key=True),
    Column("name", String(255)),
)


@pytest.fixture
def connect(make_sa_connection, loop):
    async def start():
        conn = await make_sa_connection()
        await conn.execute("DROP TABLE IF EXISTS sa_tbl")
        await conn.execute(
            "CREATE TABLE sa_tbl (id serial, name varchar(255))"
        )

        await conn.execute(tbl.insert().values(name="test_name"))

        return conn

    return loop.run_until_complete(start())


async def test_non_existing_column_error(connect):
    ret = await connect.execute(tbl.select())
    row = await ret.fetchone()
    with pytest.raises(AttributeError) as excinfo:
        row.non_existing_column
    assert excinfo.value.args == (
        "Could not locate column in row for column 'non_existing_column'",
    )
