#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
import sys

import pandas as pd
import pytest
from sqlalchemy import text

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


def test_qmark_bulk_insert(engine_testaccount_with_qmark):
    """
    Bulk insert using qmark paramstyle
    """
    if sys.version_info < (3, 8):
        pytest.skip(
            "In Python 3.7, this test depends on pandas features of which the implementation is incompatible with sqlachemy 2.0, and pandas does not support Python 3.7 anymore."
        )

    with engine_testaccount_with_qmark.connect() as con:
        with con.begin():
            con.exec_driver_sql(
                """
                create or replace table src(c1 int, c2 string) as select seq8(),
                randstr(100, random()) from table(generator(rowcount=>100000))
                """
            )
            con.exec_driver_sql("create or replace table dst like src")

            for data in pd.read_sql_query(
                text("select * from src"), con, chunksize=16000
            ):
                data.to_sql(
                    "dst", con, if_exists="append", index=False, index_label=None
                )
