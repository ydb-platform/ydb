from __future__ import annotations

from typing import cast

from tortoise import Model
from tortoise.backends.base_postgres.schema_generator import BasePostgresSchemaGenerator

from aerich.ddl import BaseDDL


class PostgresDDL(BaseDDL):
    schema_generator_cls = BasePostgresSchemaGenerator
    DIALECT = BasePostgresSchemaGenerator.DIALECT
    _ADD_INDEX_TEMPLATE = 'CREATE {unique}INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" {index_type}({column_names}){extra}'
    _DROP_INDEX_TEMPLATE = 'DROP INDEX IF EXISTS "{index_name}"'
    _ALTER_NULL_TEMPLATE = 'ALTER TABLE "{table_name}" ALTER COLUMN "{column}" {set_drop} NOT NULL'
    _MODIFY_COLUMN_TEMPLATE = (
        'ALTER TABLE "{table_name}" ALTER COLUMN "{column}" TYPE {datatype}{using}'
    )
    _SET_COMMENT_TEMPLATE = 'COMMENT ON COLUMN "{table_name}"."{column}" IS {comment}'
    _DROP_FK_TEMPLATE = 'ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS "{fk_name}"'

    def alter_column_null(self, model: type[Model], field_describe: dict) -> str:
        db_table = model._meta.db_table
        return self._ALTER_NULL_TEMPLATE.format(
            table_name=db_table,
            column=field_describe.get("db_column"),
            set_drop="DROP" if field_describe.get("nullable") else "SET",
        )

    def modify_column(self, model: type[Model], field_describe: dict, is_pk: bool = False) -> str:
        db_table = model._meta.db_table
        db_field_types = cast(dict, field_describe.get("db_field_types"))
        db_column = field_describe.get("db_column")
        datatype = db_field_types.get(self.DIALECT) or db_field_types.get("")
        return self._MODIFY_COLUMN_TEMPLATE.format(
            table_name=db_table,
            column=db_column,
            datatype=datatype,
            using=f' USING "{db_column}"::{datatype}',
        )

    def set_comment(self, model: type[Model], field_describe: dict) -> str:
        db_table = model._meta.db_table
        return self._SET_COMMENT_TEMPLATE.format(
            table_name=db_table,
            column=field_describe.get("db_column") or field_describe.get("raw_field"),
            comment=(
                "'{}'".format(field_describe.get("description"))
                if field_describe.get("description")
                else "NULL"
            ),
        )

    def drop_unique_index(
        self,
        model: type[Model],
        field_name: str,
    ) -> list[str]:
        # When change unique to be true for exists column, it's a normal index
        drop_normal_index = self.drop_index(model, [field_name], unique=True)
        # While add a new column with unique=True, it's a unique constraint
        table_name = self.get_table_name(model)
        contraint_name = f"{table_name}_{field_name}_key"
        drop_constraint = self.drop_unique_constraint(model, contraint_name)
        # To avoid connecting db to validate INDEX/CONSTRAINT, drop both of them
        # as the templates of drop index/contraints are using 'IF EXISTS'.
        return [drop_normal_index, drop_constraint]
