#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import sqlalchemy.types as sqltypes
import sqlalchemy.util as util

TEXT = sqltypes.VARCHAR
CHARACTER = sqltypes.CHAR
DEC = sqltypes.DECIMAL
DOUBLE = sqltypes.FLOAT
FIXED = sqltypes.DECIMAL
NUMBER = sqltypes.DECIMAL
BYTEINT = sqltypes.SMALLINT
STRING = sqltypes.VARCHAR
TINYINT = sqltypes.SMALLINT
VARBINARY = sqltypes.BINARY


def _process_float(value):
    if value == float("inf"):
        return "inf"
    elif value == float("-inf"):
        return "-inf"
    elif value is not None:
        return float(value)
    return value


class SnowflakeType(sqltypes.TypeEngine):
    def _default_dialect(self):
        # Get around circular import
        return __import__("snowflake.sqlalchemy").sqlalchemy.dialect()


class VARIANT(SnowflakeType):
    __visit_name__ = "VARIANT"


class OBJECT(SnowflakeType):
    __visit_name__ = "OBJECT"


class ARRAY(SnowflakeType):
    __visit_name__ = "ARRAY"


class TIMESTAMP_TZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_TZ"


class TIMESTAMP_LTZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_LTZ"


class TIMESTAMP_NTZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_NTZ"


class GEOGRAPHY(SnowflakeType):
    __visit_name__ = "GEOGRAPHY"


class GEOMETRY(SnowflakeType):
    __visit_name__ = "GEOMETRY"


class _CUSTOM_Date(SnowflakeType, sqltypes.Date):
    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                return f"'{value.isoformat()}'"

        return process


class _CUSTOM_DateTime(SnowflakeType, sqltypes.DateTime):
    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                datetime_str = value.isoformat(" ", timespec="microseconds")
                return f"'{datetime_str}'"

        return process


class _CUSTOM_Time(SnowflakeType, sqltypes.Time):
    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                time_str = value.isoformat(timespec="microseconds")
                return f"'{time_str}'"

        return process


class _CUSTOM_Float(SnowflakeType, sqltypes.Float):
    def bind_processor(self, dialect):
        return _process_float


class _CUSTOM_DECIMAL(SnowflakeType, sqltypes.DECIMAL):
    @util.memoized_property
    def _type_affinity(self):
        return sqltypes.INTEGER if self.scale == 0 else sqltypes.DECIMAL
