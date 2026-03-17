from __future__ import annotations

import re
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

from tortoise.backends.base.schema_generator import BaseSchemaGenerator

from aerich._compat import tortoise_version_less_than
from aerich.utils import is_default_function

if TYPE_CHECKING:
    from tortoise import BaseDBAsyncClient, Model


class BaseDDL:
    schema_generator_cls: type[BaseSchemaGenerator] = BaseSchemaGenerator
    DIALECT = "sql"
    _DROP_TABLE_TEMPLATE = 'DROP TABLE IF EXISTS "{table_name}"'
    _ADD_COLUMN_TEMPLATE = 'ALTER TABLE "{table_name}" ADD {column}'
    _DROP_COLUMN_TEMPLATE = 'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'
    _ALTER_DEFAULT_TEMPLATE = 'ALTER TABLE "{table_name}" ALTER COLUMN "{column}" {default}'
    _RENAME_COLUMN_TEMPLATE = (
        'ALTER TABLE "{table_name}" RENAME COLUMN "{old_column_name}" TO "{new_column_name}"'
    )
    _ADD_INDEX_TEMPLATE = 'ALTER TABLE "{table_name}" ADD {index_type}{unique}INDEX "{index_name}" ({column_names}){extra}'
    _DROP_INDEX_TEMPLATE = 'ALTER TABLE "{table_name}" DROP INDEX IF EXISTS "{index_name}"'
    _DROP_CONSTRAINT_TEMPLATE = 'ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS "{name}"'
    _ADD_FK_TEMPLATE = 'ALTER TABLE "{table_name}" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ("{db_column}") REFERENCES "{table}" ("{field}") ON DELETE {on_delete}'
    _DROP_FK_TEMPLATE = 'ALTER TABLE "{table_name}" DROP FOREIGN KEY "{fk_name}"'
    _M2M_TABLE_TEMPLATE = (
        'CREATE TABLE "{table_name}" (\n'
        '    "{backward_key}" {backward_type} NOT NULL REFERENCES "{backward_table}" ("{backward_field}") ON DELETE CASCADE,\n'
        '    "{forward_key}" {forward_type} NOT NULL REFERENCES "{forward_table}" ("{forward_field}") ON DELETE {on_delete}\n'
        "){extra}{comment}"
    )
    _MODIFY_COLUMN_TEMPLATE = 'ALTER TABLE "{table_name}" MODIFY COLUMN {column}'
    _CHANGE_COLUMN_TEMPLATE = (
        'ALTER TABLE "{table_name}" CHANGE {old_column_name} {new_column_name} {new_column_type}'
    )
    _RENAME_TABLE_TEMPLATE = 'ALTER TABLE "{old_table_name}" RENAME TO "{new_table_name}"'

    def __init__(self, client: BaseDBAsyncClient) -> None:
        self.client = client
        self.schema_generator = self.schema_generator_cls(client)

    @staticmethod
    def get_table_name(model: type[Model]) -> str:
        return model._meta.db_table

    def create_table(self, model: type[Model]) -> str:
        schema = self.schema_generator._get_table_sql(model, True)["table_creation_string"]
        if tortoise_version_less_than("0.23.1"):
            # Remove extra space
            schema = re.sub(r'(["()A-Za-z])  (["()A-Za-z])', r"\1 \2", schema)
        return schema.rstrip(";")

    def drop_table(self, table_name: str) -> str:
        return self._DROP_TABLE_TEMPLATE.format(table_name=table_name)

    def drop_m2m(self, table_name: str) -> str:
        return self.drop_table(table_name)

    def create_m2m(
        self, model: type[Model], field_describe: dict, reference_table_describe: dict
    ) -> str:
        through = cast(str, field_describe.get("through"))
        description = field_describe.get("description")
        pk_field = cast(dict, reference_table_describe.get("pk_field"))
        reference_id = pk_field.get("db_column")
        db_field_types = cast(dict, pk_field.get("db_field_types"))
        return self._M2M_TABLE_TEMPLATE.format(
            table_name=through,
            backward_table=model._meta.db_table,
            forward_table=reference_table_describe.get("table"),
            backward_field=model._meta.db_pk_column,
            forward_field=reference_id,
            backward_key=field_describe.get("backward_key"),
            backward_type=model._meta.pk.get_for_dialect(self.DIALECT, "SQL_TYPE"),
            forward_key=field_describe.get("forward_key"),
            forward_type=db_field_types.get(self.DIALECT) or db_field_types.get(""),
            on_delete=field_describe.get("on_delete"),
            extra=self.schema_generator._table_generate_extra(table=through),
            comment=(
                self.schema_generator._table_comment_generator(table=through, comment=description)
                if description
                else ""
            ),
        )

    def _get_default(self, model: type[Model], field_describe: dict) -> Any:
        db_table = model._meta.db_table
        default = field_describe.get("default")
        if isinstance(default, Enum):
            default = default.value
        db_column = cast(str, field_describe.get("db_column"))
        auto_now_add = field_describe.get("auto_now_add", False)
        auto_now = field_describe.get("auto_now", False)
        if default is not None or auto_now_add:
            if field_describe.get("field_type") in [
                "UUIDField",
                "TextField",
                "JSONField",
            ] or is_default_function(default):
                default = ""
            else:
                try:
                    default = self.schema_generator._column_default_generator(
                        db_table,
                        db_column,
                        self.schema_generator._escape_default_value(default),
                        auto_now_add,
                        auto_now,
                    )
                except NotImplementedError:
                    default = ""
        return default

    def add_column(self, model: type[Model], field_describe: dict, is_pk: bool = False) -> str:
        return self._add_or_modify_column(model, field_describe, is_pk)

    def _add_or_modify_column(
        self, model: type[Model], field_describe: dict, is_pk: bool, modify: bool = False
    ) -> str:
        db_table = model._meta.db_table
        description = field_describe.get("description")
        db_column = cast(str, field_describe.get("db_column"))
        db_field_types = cast(dict[str, str], field_describe.get("db_field_types"))
        default = self._get_default(model, field_describe)
        if default is None:
            default = ""
        if modify:
            unique = ""
            template = self._MODIFY_COLUMN_TEMPLATE
        else:
            # sqlite does not support alter table to add unique column
            unique = " UNIQUE" if field_describe.get("unique") and self.DIALECT != "sqlite" else ""
            template = self._ADD_COLUMN_TEMPLATE
        column = self.schema_generator._create_string(
            db_column=db_column,
            field_type=db_field_types.get(self.DIALECT) or db_field_types[""],
            nullable=" NOT NULL" if not field_describe.get("nullable") else "",
            unique=unique,
            comment=(
                self.schema_generator._column_comment_generator(
                    table=db_table,
                    column=db_column,
                    comment=description,
                )
                if description
                else ""
            ),
            is_primary_key=is_pk,
            default=default,
        )
        if tortoise_version_less_than("0.23.1"):
            column = column.replace("  ", " ")
        return template.format(table_name=db_table, column=column)

    def should_add_unique_index_when_adding_column(self) -> bool:
        # mysql/postgres use 'Add new_field ... NOT NULL UNIQUE' to set unique constraint
        return self.DIALECT == "sqlite"

    def drop_column(self, model: type[Model], column_name: str) -> str:
        return self._DROP_COLUMN_TEMPLATE.format(
            table_name=model._meta.db_table, column_name=column_name
        )

    def modify_column(self, model: type[Model], field_describe: dict, is_pk: bool = False) -> str:
        return self._add_or_modify_column(model, field_describe, is_pk, modify=True)

    def rename_column(self, model: type[Model], old_column_name: str, new_column_name: str) -> str:
        return self._RENAME_COLUMN_TEMPLATE.format(
            table_name=model._meta.db_table,
            old_column_name=old_column_name,
            new_column_name=new_column_name,
        )

    def change_column(
        self, model: type[Model], old_column_name: str, new_column_name: str, new_column_type: str
    ) -> str:
        return self._CHANGE_COLUMN_TEMPLATE.format(
            table_name=model._meta.db_table,
            old_column_name=old_column_name,
            new_column_name=new_column_name,
            new_column_type=new_column_type,
        )

    def _index_name(self, unique: bool | None, model: type[Model], field_names: list[str]) -> str:
        func_name = "_get_index_name"
        if not hasattr(self.schema_generator, func_name):
            # For tortoise-orm<0.24.1
            func_name = "_generate_index_name"
        return getattr(self.schema_generator, func_name)(
            "idx" if not unique else "uid", model, field_names
        )

    def add_index(
        self,
        model: type[Model],
        field_names: list[str],
        unique: bool | None = False,
        name: str | None = None,
        index_type: str = "",
        extra: str | None = "",
    ) -> str:
        return self._ADD_INDEX_TEMPLATE.format(
            unique="UNIQUE " if unique else "",
            index_name=name or self._index_name(unique, model, field_names),
            table_name=model._meta.db_table,
            column_names=", ".join(self.schema_generator.quote(f) for f in field_names),
            index_type=f"{index_type} " if index_type else "",
            extra=f"{extra}" if extra else "",
        )

    def drop_index(
        self,
        model: type[Model],
        field_names: list[str],
        unique: bool | None = False,
        name: str | None = None,
    ) -> str:
        return self._DROP_INDEX_TEMPLATE.format(
            index_name=name or self._index_name(unique, model, field_names),
            table_name=model._meta.db_table,
        )

    def drop_index_by_name(self, model: type[Model], index_name: str) -> str:
        return self.drop_index(model, [], name=index_name)

    def drop_unique_constraint(self, model: type[Model], name: str) -> str:
        return self._DROP_CONSTRAINT_TEMPLATE.format(
            table_name=model._meta.db_table,
            name=name,
        )

    def _generate_fk_name(
        self, db_table: str, field_describe: dict, reference_table_describe: dict
    ) -> str:
        """Generate fk name"""
        db_column = cast(str, field_describe.get("raw_field"))
        pk_field = cast(dict, reference_table_describe.get("pk_field"))
        to_field = cast(str, pk_field.get("db_column"))
        to_table = cast(str, reference_table_describe.get("table"))
        func_name = "_get_fk_name"
        if not hasattr(self.schema_generator, func_name):
            # For tortoise-orm<0.24.1
            func_name = "_generate_fk_name"
        return getattr(self.schema_generator, func_name)(
            from_table=db_table,
            from_field=db_column,
            to_table=to_table,
            to_field=to_field,
        )

    def add_fk(
        self, model: type[Model], field_describe: dict, reference_table_describe: dict
    ) -> str:
        db_table = model._meta.db_table

        db_column = field_describe.get("raw_field")
        pk_field = cast(dict, reference_table_describe.get("pk_field"))
        reference_id = pk_field.get("db_column")
        return self._ADD_FK_TEMPLATE.format(
            table_name=db_table,
            fk_name=self._generate_fk_name(db_table, field_describe, reference_table_describe),
            db_column=db_column,
            table=reference_table_describe.get("table"),
            field=reference_id,
            on_delete=field_describe.get("on_delete"),
        )

    def drop_fk(
        self, model: type[Model], field_describe: dict, reference_table_describe: dict
    ) -> str:
        db_table = model._meta.db_table
        fk_name = self._generate_fk_name(db_table, field_describe, reference_table_describe)
        return self._DROP_FK_TEMPLATE.format(table_name=db_table, fk_name=fk_name)

    def alter_column_default(self, model: type[Model], field_describe: dict) -> str:
        db_table = model._meta.db_table
        default = self._get_default(model, field_describe)
        return self._ALTER_DEFAULT_TEMPLATE.format(
            table_name=db_table,
            column=field_describe.get("db_column"),
            default="SET" + default if default is not None else "DROP DEFAULT",
        )

    def alter_column_null(self, model: type[Model], field_describe: dict) -> str:
        return self.modify_column(model, field_describe)

    def set_comment(self, model: type[Model], field_describe: dict) -> str:
        return self.modify_column(model, field_describe)

    def rename_table(self, model: type[Model], old_table_name: str, new_table_name: str) -> str:
        db_table = model._meta.db_table
        return self._RENAME_TABLE_TEMPLATE.format(
            table_name=db_table, old_table_name=old_table_name, new_table_name=new_table_name
        )

    def alter_indexed_column_unique(
        self, model: type[Model], field_name: str, drop: bool = False
    ) -> list[str]:
        """Change unique constraint for indexed field, e.g.: Field(db_index=True) --> Field(unique=True)"""
        fields = [field_name]
        if drop:
            drop_unique = self.drop_index(model, fields, unique=True)
            add_normal_index = self.add_index(model, fields, unique=False)
            return [drop_unique, add_normal_index]
        else:
            drop_index = self.drop_index(model, fields, unique=False)
            add_unique_index = self.add_index(model, fields, unique=True)
            return [drop_index, add_unique_index]
