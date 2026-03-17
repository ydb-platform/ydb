from __future__ import annotations

from tortoise import Model
from tortoise.backends.sqlite.schema_generator import SqliteSchemaGenerator

from aerich.ddl import BaseDDL
from aerich.exceptions import NotSupportError


class SqliteDDL(BaseDDL):
    schema_generator_cls = SqliteSchemaGenerator
    DIALECT = SqliteSchemaGenerator.DIALECT
    _ADD_INDEX_TEMPLATE = 'CREATE {unique}INDEX "{index_name}" ON "{table_name}" ({column_names})'
    _DROP_INDEX_TEMPLATE = 'DROP INDEX IF EXISTS "{index_name}"'

    def modify_column(self, model: type[Model], field_object: dict, is_pk: bool = True):
        raise NotSupportError("Modify column is unsupported in SQLite.")

    def alter_column_default(self, model: type[Model], field_describe: dict):
        raise NotSupportError("Alter column default is unsupported in SQLite.")

    def alter_column_null(self, model: type[Model], field_describe: dict):
        raise NotSupportError("Alter column null is unsupported in SQLite.")

    def set_comment(self, model: type[Model], field_describe: dict):
        raise NotSupportError("Alter column comment is unsupported in SQLite.")
