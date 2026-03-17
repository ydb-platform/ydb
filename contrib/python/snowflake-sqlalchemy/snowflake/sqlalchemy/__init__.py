#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import sys

if sys.version_info < (3, 8):
    import importlib_metadata
else:
    import importlib.metadata as importlib_metadata

from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INT,
    INTEGER,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    VARCHAR,
)

from . import base, snowdialect
from .custom_commands import (
    AWSBucket,
    AzureContainer,
    CopyFormatter,
    CopyIntoStorage,
    CreateFileFormat,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    JSONFormatter,
    MergeInto,
    PARQUETFormatter,
)
from .custom_types import (
    ARRAY,
    BYTEINT,
    CHARACTER,
    DEC,
    DOUBLE,
    FIXED,
    GEOGRAPHY,
    GEOMETRY,
    NUMBER,
    OBJECT,
    STRING,
    TEXT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    TINYINT,
    VARBINARY,
    VARIANT,
)
from .util import _url as URL

base.dialect = dialect = snowdialect.dialect

__version__ = importlib_metadata.version("snowflake-sqlalchemy")

__all__ = (
    "BIGINT",
    "BINARY",
    "BOOLEAN",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "FLOAT",
    "INT",
    "INTEGER",
    "REAL",
    "SMALLINT",
    "TIME",
    "TIMESTAMP",
    "URL",
    "VARCHAR",
    "ARRAY",
    "BYTEINT",
    "CHARACTER",
    "DEC",
    "DOUBLE",
    "FIXED",
    "GEOGRAPHY",
    "GEOMETRY",
    "OBJECT",
    "NUMBER",
    "STRING",
    "TEXT",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "TIMESTAMP_NTZ",
    "TINYINT",
    "VARBINARY",
    "VARIANT",
    "MergeInto",
    "CSVFormatter",
    "JSONFormatter",
    "PARQUETFormatter",
    "CopyFormatter",
    "CopyIntoStorage",
    "AWSBucket",
    "AzureContainer",
    "ExternalStage",
    "CreateStage",
    "CreateFileFormat",
)
