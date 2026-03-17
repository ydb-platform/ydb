from typing import Any

from .base import (
    BIGINT as BIGINT,
    BINARY as BINARY,
    BIT as BIT,
    BLOB as BLOB,
    BOOLEAN as BOOLEAN,
    CHAR as CHAR,
    DATE as DATE,
    DATETIME as DATETIME,
    DECIMAL as DECIMAL,
    DOUBLE as DOUBLE,
    ENUM as ENUM,
    DECIMAL as DECIMAL,
    FLOAT as FLOAT,
    INTEGER as INTEGER,
    INTEGER as INTEGER,
    JSON as JSON,
    LONGBLOB as LONGBLOB,
    LONGTEXT as LONGTEXT,
    MEDIUMBLOB as MEDIUMBLOB,
    MEDIUMINT as MEDIUMINT,
    MEDIUMTEXT as MEDIUMTEXT,
    NCHAR as NCHAR,
    NVARCHAR as NVARCHAR,
    NUMERIC as NUMERIC,
    SET as SET,
    SMALLINT as SMALLINT,
    REAL as REAL,
    TEXT as TEXT,
    TIME as TIME,
    TIMESTAMP as TIMESTAMP,
    TINYBLOB as TINYBLOB,
    TINYINT as TINYINT,
    TINYTEXT as TINYTEXT,
    VARBINARY as VARBINARY,
    VARCHAR as VARCHAR,
    YEAR as YEAR,
)

from .mysqldb import dialect as dialect

# This should stay here until we are sure we added all submodules,
# to avoid false positives.
def __getattr__(attr: str) -> Any: ...
