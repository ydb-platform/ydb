#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy import Integer, Sequence, String
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.sql import select


def test_insert_table(engine_testaccount):
    metadata = MetaData()
    users = Table(
        "users",
        metadata,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )
    metadata.create_all(engine_testaccount)

    data = [
        {
            "id": 1,
            "name": "testname1",
            "fullname": "fulltestname1",
        },
        {
            "id": 2,
            "name": "testname2",
            "fullname": "fulltestname2",
        },
    ]
    try:
        with engine_testaccount.connect() as conn:
            # using multivalue insert
            with conn.begin():
                conn.execute(users.insert().values(data))
                results = conn.execute(select(users).order_by("id"))
                row = results.fetchone()
                assert row._mapping["name"] == "testname1"

    finally:
        users.drop(engine_testaccount)
