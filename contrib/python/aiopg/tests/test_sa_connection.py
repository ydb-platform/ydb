from unittest import mock

import psycopg2
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, func, select
from sqlalchemy.schema import CreateTable, DropTable

from aiopg import Cursor, sa

meta = MetaData()
tbl = Table(
    "sa_tbl",
    meta,
    Column("id", Integer, nullable=False, primary_key=True),
    Column("name", String(255)),
)


@pytest.fixture
def connect(make_connection):
    async def go(**kwargs):
        conn = await make_connection(**kwargs)
        cur = await conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS sa_tbl")
        await cur.execute(
            "CREATE TABLE sa_tbl " "(id serial, name varchar(255))"
        )
        await cur.execute("INSERT INTO sa_tbl (name)" "VALUES ('first')")
        cur.close()

        engine = mock.Mock(from_spec=sa.engine.Engine)
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)

    yield go


async def test_execute_text_select(connect):
    conn = await connect()
    res = await conn.execute("SELECT * FROM sa_tbl;")
    assert isinstance(res.cursor, Cursor)
    assert ("id", "name") == res.keys()
    rows = await res.fetchall()
    assert res.closed
    assert res.cursor is None
    assert 1 == len(rows)
    row = rows[0]
    assert 1 == row[0]
    assert 1 == row["id"]
    assert 1 == row.id
    assert "first" == row[1]
    assert "first" == row["name"]
    assert "first" == row.name


async def test_execute_sa_select(connect):
    conn = await connect()
    res = await conn.execute(tbl.select())
    assert isinstance(res.cursor, Cursor)
    assert ("id", "name") == res.keys()
    rows = await res.fetchall()
    assert res.closed
    assert res.cursor is None
    assert res.returns_rows

    assert 1 == len(rows)
    row = rows[0]
    assert 1 == row[0]
    assert 1 == row["id"]
    assert 1 == row.id
    assert "first" == row[1]
    assert "first" == row["name"]
    assert "first" == row.name


async def test_execute_sa_select_with_in(connect):
    conn = await connect()
    await conn.execute(tbl.insert(), 2, "second")
    await conn.execute(tbl.insert(), 3, "third")

    res = await conn.execute(
        tbl.select().where(tbl.c.name.in_(["first", "second"]))
    )
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, "first") == rows[0]
    assert (2, "second") == rows[1]


async def test_execute_sa_insert_with_dict(connect):
    conn = await connect()
    await conn.execute(tbl.insert(), {"id": 2, "name": "second"})

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, "first") == rows[0]
    assert (2, "second") == rows[1]


async def test_execute_sa_insert_with_tuple(connect):
    conn = await connect()
    await conn.execute(tbl.insert(), (2, "second"))

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, "first") == rows[0]
    assert (2, "second") == rows[1]


async def test_execute_sa_insert_named_params(connect):
    conn = await connect()
    await conn.execute(tbl.insert(), id=2, name="second")

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, "first") == rows[0]
    assert (2, "second") == rows[1]


async def test_execute_sa_insert_positional_params(connect):
    conn = await connect()
    await conn.execute(tbl.insert(), 2, "second")

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, "first") == rows[0]
    assert (2, "second") == rows[1]


async def test_scalar(connect):
    conn = await connect()
    res = await conn.scalar(select([func.count()]).select_from(tbl))
    assert 1, res


async def test_scalar_None(connect):
    conn = await connect()
    await conn.execute(tbl.delete())
    res = await conn.scalar(tbl.select())
    assert res is None


async def test_row_proxy(connect):
    conn = await connect()
    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    row = rows[0]
    row2 = await (await conn.execute(tbl.select())).first()
    assert 2 == len(row)
    assert ["id", "name"] == list(row)
    assert "id" in row
    assert "unknown" not in row
    assert "first" == row.name
    assert "first" == row[tbl.c.name]
    with pytest.raises(AttributeError):
        row.unknown
    assert "(1, 'first')" == repr(row)
    assert (1, "first") == row.as_tuple()
    assert (555, "other") != row.as_tuple()
    assert row2 == row
    assert not (row2 != row)
    assert 5 != row


async def test_insert(connect):
    conn = await connect()
    res = await conn.execute(tbl.insert().values(name="second"))
    assert ("id",) == res.keys()
    assert 1 == res.rowcount
    assert res.returns_rows

    rows = await res.fetchall()
    assert 1 == len(rows)
    assert 2 == rows[0].id


async def test_raw_insert(connect):
    conn = await connect()
    await conn.execute("INSERT INTO sa_tbl (name) VALUES ('third')")
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ("id", "name") == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


async def test_raw_insert_with_params(connect):
    conn = await connect()
    res = await conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%s, %s)", 2, "third"
    )
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ("id", "name") == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


async def test_raw_insert_with_params_dict(connect):
    conn = await connect()
    res = await conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
        {"id": 2, "name": "third"},
    )
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ("id", "name") == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


async def test_raw_insert_with_named_params(connect):
    conn = await connect()
    res = await conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
        id=2,
        name="third",
    )
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ("id", "name") == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


async def test_raw_insert_with_executemany(connect):
    conn = await connect()
    with pytest.raises(sa.ArgumentError):
        await conn.execute(
            "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
            [(2, "third"), (3, "forth")],
        )


async def test_delete(connect):
    conn = await connect()
    res = await conn.execute(tbl.delete().where(tbl.c.id == 1))
    assert () == res.keys()
    assert 1 == res.rowcount
    assert not res.returns_rows
    assert res.closed
    assert res.cursor is None


async def test_double_close(connect):
    conn = await connect()
    res = await conn.execute("SELECT 1")
    res.close()
    assert res.closed
    assert res.cursor is None
    res.close()
    assert res.closed
    assert res.cursor is None


async def test_fetchall(connect):
    conn = await connect()
    await conn.execute(tbl.insert().values(name="second"))

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert res.closed
    assert res.returns_rows
    assert [(1, "first") == (2, "second")], rows


async def test_fetchall_closed(connect):
    conn = await connect()
    await conn.execute(tbl.insert().values(name="second"))

    res = await conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchall()


async def test_fetchall_not_returns_rows(connect):
    conn = await connect()
    res = await conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchall()


async def test_fetchone_closed(connect):
    conn = await connect()
    await conn.execute(tbl.insert().values(name="second"))

    res = await conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchone()


async def test_first_not_returns_rows(connect):
    conn = await connect()
    res = await conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        await res.first()


async def test_fetchmany(connect):
    conn = await connect()
    await conn.execute(tbl.insert().values(name="second"))

    res = await conn.execute(tbl.select())
    rows = await res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, "first")] == rows


async def test_fetchmany_with_size(connect):
    conn = await connect()
    await conn.execute(tbl.insert().values(name="second"))

    res = await conn.execute(tbl.select())
    rows = await res.fetchmany(100)
    assert 2 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, "first") == (2, "second")], rows


async def test_fetchmany_closed(connect):
    conn = await connect()
    await conn.execute(tbl.insert().values(name="second"))

    res = await conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()


async def test_fetchmany_with_size_closed(connect):
    conn = await connect()
    await conn.execute(tbl.insert().values(name="second"))

    res = await conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany(5555)


async def test_fetchmany_not_returns_rows(connect):
    conn = await connect()
    res = await conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()


async def test_fetchmany_close_after_last_read(connect):
    conn = await connect()

    res = await conn.execute(tbl.select())
    rows = await res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, "first")] == rows
    rows2 = await res.fetchmany()
    assert 0 == len(rows2)
    assert res.closed


async def test_create_table(connect):
    conn = await connect()
    res = await conn.execute(DropTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.execute("SELECT * FROM sa_tbl")

    res = await conn.execute(CreateTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()

    res = await conn.execute("SELECT * FROM sa_tbl")
    assert 0 == len(await res.fetchall())


async def test_execute_when_closed(connect):
    conn = await connect()
    await conn.close()

    with pytest.raises(sa.ResourceClosedError):
        await conn.execute(tbl.select())
