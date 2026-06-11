from sqlalchemy import Table
from sqlalchemy.dialects import registry

from clickhouse_connect import driver_name
from clickhouse_connect.cc_sqlalchemy import types
from clickhouse_connect.cc_sqlalchemy.datatypes.base import schema_types
from clickhouse_connect.cc_sqlalchemy.ddl import tableengine as engines
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.sql import final, sample
from clickhouse_connect.cc_sqlalchemy.sql.clauses import ArrayJoin, ClickHouseJoin, Lambda, array_join, ch_join
from clickhouse_connect.dbapi.cursor import Cursor

registry.register("clickhouse", "clickhouse_connect.cc_sqlalchemy.dialect", "ClickHouseDialect")
registry.register("clickhouse.connect", "clickhouse_connect.cc_sqlalchemy.dialect", "ClickHouseDialect")

dialect_name = driver_name
ischema_names = schema_types

CH_DIALECT = dialect_name
ClickhouseDictionary = Dictionary

__all__ = [
    "dialect_name",
    "CH_DIALECT",
    "ischema_names",
    "array_join",
    "ArrayJoin",
    "ch_join",
    "ClickHouseJoin",
    "Lambda",
    "final",
    "sample",
    "Dictionary",
    "ClickhouseDictionary",
    "engines",
    "types",
    "Cursor",
    "Table",
]
