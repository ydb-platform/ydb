#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from datetime import datetime

import pytz
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.sql import select

from snowflake.sqlalchemy import TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ

PST_TZ = "America/Los_Angeles"
JST_TZ = "Asia/Tokyo"


def test_create_table_timestamp_datatypes(engine_testaccount):
    """
    Create table including timestamp data types
    """
    metadata = MetaData()
    table_name = "test_timestamp0"
    test_timestamp = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("tsntz", TIMESTAMP_NTZ),
        Column("tsltz", TIMESTAMP_LTZ),
        Column("tstz", TIMESTAMP_TZ),
    )
    metadata.create_all(engine_testaccount)
    try:
        assert test_timestamp is not None
    finally:
        test_timestamp.drop(engine_testaccount)


def test_inspect_timestamp_datatypes(engine_testaccount):
    """
    Create table including timestamp data types
    """
    metadata = MetaData()
    table_name = "test_timestamp0"
    test_timestamp = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("tsntz", TIMESTAMP_NTZ),
        Column("tsltz", TIMESTAMP_LTZ),
        Column("tstz", TIMESTAMP_TZ),
    )
    metadata.create_all(engine_testaccount)
    try:
        current_utctime = datetime.utcnow()
        current_localtime = pytz.utc.localize(current_utctime, is_dst=False).astimezone(
            pytz.timezone(PST_TZ)
        )
        current_localtime_without_tz = datetime.now()
        current_localtime_with_other_tz = pytz.utc.localize(
            current_localtime_without_tz, is_dst=False
        ).astimezone(pytz.timezone(JST_TZ))

        ins = test_timestamp.insert().values(
            id=1,
            tsntz=current_utctime,
            tsltz=current_localtime,
            tstz=current_localtime_with_other_tz,
        )
        with engine_testaccount.connect() as conn:
            with conn.begin():
                results = conn.execute(ins)
                results.close()

                s = select(test_timestamp)
                results = conn.execute(s)
                rows = results.fetchone()
                results.close()
                assert rows[0] == 1
                assert rows[1] == current_utctime
                assert rows[2] == current_localtime
                assert rows[3] == current_localtime_with_other_tz
    finally:
        test_timestamp.drop(engine_testaccount)
