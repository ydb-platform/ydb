from enum import Enum

import psycopg2
import pytest
from sqlalchemy import Column, Integer, MetaData, Table, types
from sqlalchemy.dialects.postgresql import ARRAY, ENUM, HSTORE, JSON
from sqlalchemy.schema import CreateTable, DropTable

sa = pytest.importorskip("aiopg.sa")  # noqa

meta = MetaData()


class SimpleEnum(Enum):
    first = "first"
    second = "second"


class PythonEnum(types.TypeDecorator):
    impl = types.Enum

    def __init__(self, python_enum_type, **kwargs):
        self.python_enum_type = python_enum_type
        self.kwargs = kwargs
        enum_args = [x.value for x in python_enum_type]
        super().__init__(*enum_args, **self.kwargs)

    def process_bind_param(self, value, dialect):
        return value.value

    def process_result_value(self, value: str, dialect):
        for __, case in self.python_enum_type.__members__.items():
            if case.value == value:
                return case
        raise TypeError(
            f"Cannot map Enum value {value!r} "
            f"to Python's {self.python_enum_type}"
        )

    def copy(self):
        return PythonEnum(self.python_enum_type, **self.kwargs)


tbl = Table(
    "sa_tbl_types",
    meta,
    Column("id", Integer, nullable=False, primary_key=True),
    Column("json_val", JSON),
    Column("array_val", ARRAY(Integer)),
    Column("hstore_val", HSTORE),
    Column("pyt_enum_val", PythonEnum(SimpleEnum, name="simple_enum")),
    Column("enum_val", ENUM("first", "second", name="simple_enum")),
)

tbl2 = Table(
    "sa_tbl_types2",
    meta,
    Column("id", Integer, nullable=False, primary_key=True),
    Column("json_val", JSON),
    Column("array_val", ARRAY(Integer)),
    Column("pyt_enum_val", PythonEnum(SimpleEnum, name="simple_enum")),
    Column("enum_val", ENUM("first", "second", name="simple_enum")),
)


@pytest.fixture
def connect(make_engine):
    async def go(**kwargs):
        engine = await make_engine(**kwargs)
        with (await engine) as conn:
            try:
                await conn.execute(DropTable(tbl))
            except psycopg2.ProgrammingError:
                pass
            try:
                await conn.execute(DropTable(tbl2))
            except psycopg2.ProgrammingError:
                pass
            await conn.execute("DROP TYPE IF EXISTS simple_enum CASCADE;")
            await conn.execute(
                """CREATE TYPE simple_enum AS ENUM
                                       ('first', 'second');"""
            )
            try:
                await conn.execute(CreateTable(tbl))
                ret_tbl = tbl
                has_hstore = True
            except psycopg2.ProgrammingError:
                await conn.execute(CreateTable(tbl2))
                ret_tbl = tbl2
                has_hstore = False
        return engine, ret_tbl, has_hstore

    yield go


async def test_json(connect):
    engine, tbl, has_hstore = await connect()
    data = {"a": 1, "b": "name"}
    with (await engine) as conn:
        await conn.execute(tbl.insert().values(json_val=data))

        ret = await conn.execute(tbl.select())
        item = await ret.fetchone()
        assert data == item["json_val"]


async def test_array(connect):
    engine, tbl, has_hstore = await connect()
    data = [1, 2, 3]
    with (await engine) as conn:
        await conn.execute(tbl.insert().values(array_val=data))

        ret = await conn.execute(tbl.select())
        item = await ret.fetchone()
        assert data == item["array_val"]


async def test_hstore(connect):
    engine, tbl, has_hstore = await connect()
    if not has_hstore:
        raise pytest.skip("hstore is not supported")
    data = {"a": "str", "b": "name"}
    with (await engine) as conn:
        await conn.execute(tbl.insert().values(hstore_val=data))

        ret = await conn.execute(tbl.select())
        item = await ret.fetchone()
        assert data == item["hstore_val"]


async def test_enum(connect):
    engine, tbl, has_hstore = await connect()
    with (await engine) as conn:
        await conn.execute(tbl.insert().values(enum_val="second"))

        ret = await conn.execute(tbl.select())
        item = await ret.fetchone()
        assert "second" == item["enum_val"]


async def test_pyenum(connect):
    engine, tbl, has_hstore = await connect()
    with (await engine) as conn:
        await conn.execute(tbl.insert().values(pyt_enum_val=SimpleEnum.first))

        ret = await conn.execute(tbl.select())
        item = await ret.fetchone()
        assert SimpleEnum.first == item.pyt_enum_val
