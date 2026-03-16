#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import json
import textwrap

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, inspect
from sqlalchemy.sql import select

from snowflake.sqlalchemy import ARRAY, OBJECT, VARIANT


def test_create_table_semi_structured_datatypes(engine_testaccount):
    """
    Create table including semi-structured data types
    """
    metadata = MetaData()
    table_name = "test_variant0"
    test_variant = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("va", VARIANT),
        Column("ob", OBJECT),
        Column("ar", ARRAY),
    )
    metadata.create_all(engine_testaccount)
    try:
        assert test_variant is not None
    finally:
        test_variant.drop(engine_testaccount)


@pytest.mark.skip(
    """
Semi-structured data cannot be inserted by INSERT VALUES. Instead,
INSERT SELECT must be used. The fix should be either 1) SQLAlchemy dialect
transforms INSERT statement or 2) Snwoflake DB supports INSERT VALUES for
semi-structured data types. No ETA for this fix.
"""
)
def test_insert_semi_structured_datatypes(engine_testaccount):
    metadata = MetaData()
    table_name = "test_variant1"
    test_variant = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("va", VARIANT),
        Column("ob", OBJECT),
        Column("ar", ARRAY),
    )
    metadata.create_all(engine_testaccount)
    try:
        ins = test_variant.insert().values(id=1, va='{"vk1":100, "vk2":200, "vk3":300}')
        results = engine_testaccount.execute(ins)
        results.close()
    finally:
        test_variant.drop(engine_testaccount)


def test_inspect_semi_structured_datatypes(engine_testaccount):
    """
    Inspects semi-structured data type columns
    """
    table_name = "test_variant2"
    metadata = MetaData()
    test_variant = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("va", VARIANT),
        Column("ar", ARRAY),
    )
    metadata.create_all(engine_testaccount)
    try:
        with engine_testaccount.connect() as conn:
            with conn.begin():
                sql = textwrap.dedent(
                    f"""
                    INSERT INTO {table_name}(id, va, ar)
                    SELECT 1,
                           PARSE_JSON('{{"vk1":100, "vk2":200, "vk3":300}}'),
                           PARSE_JSON('[
                    {{"k":1, "v":"str1"}},
                    {{"k":2, "v":"str2"}},
                    {{"k":3, "v":"str3"}}]'
                    )
                    """
                )
                conn.exec_driver_sql(sql)
                inspecter = inspect(engine_testaccount)
                columns = inspecter.get_columns(table_name)
                assert isinstance(columns[1]["type"], VARIANT)
                assert isinstance(columns[2]["type"], ARRAY)

                s = select(test_variant)
                results = conn.execute(s)
                rows = results.fetchone()
                results.close()
                assert rows[0] == 1
                data = json.loads(rows[1])
                assert data["vk1"] == 100
                assert data["vk3"] == 300
                assert data is not None
                data = json.loads(rows[2])
                assert data[1]["k"] == 2
    finally:
        test_variant.drop(engine_testaccount)
