import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable

meta = sa.MetaData()
tbl = sa.Table(
    "sa_tbl4",
    meta,
    sa.Column("id", sa.Integer, nullable=False, primary_key=True),
    sa.Column(
        "id_sequence",
        sa.Integer,
        nullable=False,
        default=sa.Sequence("id_sequence_seq"),
    ),
    sa.Column("name", sa.String(255), nullable=False, default="default test"),
    sa.Column("count", sa.Integer, default=100, nullable=None),
    sa.Column("date", sa.DateTime, default=datetime.datetime.now),
    sa.Column("count_str", sa.Integer, default=sa.func.length("abcdef")),
    sa.Column("is_active", sa.Boolean, default=True),
)


@pytest.fixture
def engine(make_engine, loop):
    async def start():
        engine = await make_engine()
        with (await engine) as conn:
            await conn.execute("DROP TABLE IF EXISTS sa_tbl4")
            await conn.execute("DROP SEQUENCE IF EXISTS id_sequence_seq")
            await conn.execute(CreateTable(tbl))
            await conn.execute("CREATE SEQUENCE id_sequence_seq")

        return engine

    return loop.run_until_complete(start())


async def test_default_fields(engine):
    with (await engine) as conn:
        await conn.execute(tbl.insert().values())

        res = await conn.execute(tbl.select())
        row = await res.fetchone()
        assert row.count == 100
        assert row.id_sequence == 1
        assert row.count_str == 6
        assert row.name == "default test"
        assert row.is_active is True
        assert type(row.date) == datetime.datetime


async def test_default_fields_isnull(engine):
    with (await engine) as conn:
        await conn.execute(
            tbl.insert().values(
                is_active=False,
                date=None,
            )
        )

        res = await conn.execute(tbl.select())
        row = await res.fetchone()
        assert row.count == 100
        assert row.id_sequence == 1
        assert row.count_str == 6
        assert row.name == "default test"
        assert row.is_active is False
        assert row.date is None


async def test_default_fields_edit(engine):
    with (await engine) as conn:
        date = datetime.datetime.now()
        await conn.execute(
            tbl.insert().values(
                name="edit name",
                is_active=False,
                date=date,
                count=1,
            )
        )

        res = await conn.execute(tbl.select())
        row = await res.fetchone()
        assert row.count == 1
        assert row.id_sequence == 1
        assert row.count_str == 6
        assert row.name == "edit name"
        assert row.is_active is False
        assert row.date == date
