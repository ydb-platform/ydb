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

        return conn

    return loop.run_until_complete(start())


async def test_priority_name(connect):
    await connect.execute(tbl.insert().values(id="test_id", name="test_name"))
    row = await (await connect.execute(tbl.select())).first()
    assert row.name == "test_name"
    assert row.id == "test_id"


async def test_priority_name_label(connect):
    await connect.execute(tbl.insert().values(id="test_id", name="test_name"))
    query = sa.select([tbl.c.name.label("test_label_name"), tbl.c.id])
    query = query.select_from(tbl)
    row = await (await connect.execute(query)).first()
    assert row.test_label_name == "test_name"
    assert row.id == "test_id"


async def test_priority_name_and_label(connect):
    await connect.execute(tbl.insert().values(id="test_id", name="test_name"))
    query = sa.select(
        [tbl.c.name.label("test_label_name"), tbl.c.name, tbl.c.id]
    )
    query = query.select_from(tbl)
    row = await (await connect.execute(query)).first()
    assert row.test_label_name == "test_name"
    assert row.name == "test_name"
    assert row.id == "test_id"


async def test_priority_name_all_get(connect):
    await connect.execute(tbl.insert().values(id="test_id", name="test_name"))
    query = sa.select([tbl.c.name])
    query = query.select_from(tbl)
    row = await (await connect.execute(query)).first()
    assert row.name == "test_name"
    assert row["name"] == "test_name"
    assert row[0] == "test_name"
    assert row[tbl.c.name] == "test_name"


async def test_use_labels(connect):
    """key property is ignored"""
    await connect.execute(tbl.insert().values(id="test_id", name="test_name"))
    query = tbl.select(use_labels=True)
    row = await (await connect.execute(query)).first()
    assert row.sa_tbl5_Name == "test_name"
    assert row.sa_tbl5_ID == "test_id"

    assert not hasattr(row, "name")
    assert not hasattr(row, "id")
