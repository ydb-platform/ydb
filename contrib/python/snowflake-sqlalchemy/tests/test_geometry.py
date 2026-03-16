#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from json import loads

from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.sql import select

from snowflake.sqlalchemy import GEOMETRY


def test_create_table_geometry_datatypes(engine_testaccount):
    """
    Create table including geometry data types
    """
    metadata = MetaData()
    table_name = "test_geometry0"
    test_geometry = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", GEOMETRY),
    )
    metadata.create_all(engine_testaccount)
    try:
        assert test_geometry is not None
    finally:
        test_geometry.drop(engine_testaccount)


def test_inspect_geometry_datatypes(engine_testaccount):
    """
    Create table including geometry data types
    """
    metadata = MetaData()
    table_name = "test_geometry0"
    test_geometry = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom1", GEOMETRY),
        Column("geom2", GEOMETRY),
    )
    metadata.create_all(engine_testaccount)

    try:
        with engine_testaccount.connect() as conn:
            test_point = "POINT(-94.58473 39.08985)"
            test_point1 = '{"coordinates": [-94.58473, 39.08985],"type": "Point"}'

            ins = test_geometry.insert().values(
                id=1, geom1=test_point, geom2=test_point1
            )

            with conn.begin():
                results = conn.execute(ins)
                results.close()

                s = select(test_geometry)
                results = conn.execute(s)
                rows = results.fetchone()
                results.close()
                assert rows[0] == 1
                assert rows[1] == rows[2]
                assert loads(rows[2]) == loads(test_point1)
    finally:
        test_geometry.drop(engine_testaccount)
