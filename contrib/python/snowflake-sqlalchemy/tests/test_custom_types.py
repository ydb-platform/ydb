#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.sqlalchemy import custom_types


def test_string_conversions():
    """Makes sure that all of the Snowflake SQLAlchemy types can be turned into Strings"""
    sf_custom_types = [
        "VARIANT",
        "OBJECT",
        "ARRAY",
        "TIMESTAMP_TZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "GEOGRAPHY",
        "GEOMETRY",
    ]
    sf_types = [
        "TEXT",
        "CHARACTER",
        "DEC",
        "DOUBLE",
        "FIXED",
        "NUMBER",
        "BYTEINT",
        "STRING",
        "TINYINT",
        "VARBINARY",
    ] + sf_custom_types

    for type_ in sf_types:
        sample = getattr(custom_types, type_)()
        if type_ in sf_custom_types:
            assert type_ == str(sample)
