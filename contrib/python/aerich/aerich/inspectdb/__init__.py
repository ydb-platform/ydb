from __future__ import annotations

import contextlib
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, TypedDict

from tortoise import BaseDBAsyncClient

from aerich.exceptions import NotSupportError


class ColumnInfoDict(TypedDict):
    name: str
    pk: str
    index: str
    null: str
    default: str
    length: str
    comment: str


FieldMapDict = dict[str, Callable[..., str]]


@dataclass
class Column:
    name: str
    data_type: str
    null: bool
    default: Any
    pk: bool
    unique: bool
    index: bool
    comment: str | None = None
    length: int | None = None
    extra: str | None = None
    decimal_places: int | None = None
    max_digits: int | None = None

    @staticmethod
    def trans_default(value: str, data_type: str, extra: str | None) -> str:
        if data_type in ("tinyint", "INT"):
            default = f"default={'True' if value == '1' else 'False'}, "
        elif data_type == "bool":
            default = f"default={'True' if value == 'true' else 'False'}, "
        elif data_type in ("datetime", "timestamptz", "TIMESTAMP"):
            if value == "CURRENT_TIMESTAMP":
                if extra == "DEFAULT_GENERATED on update CURRENT_TIMESTAMP":
                    default = "auto_now=True, "
                else:
                    default = "auto_now_add=True, "
            else:
                default = ""
        else:
            if "::" in value:
                default = f"default={value.split('::')[0]}, "
            elif value.endswith("()"):
                default = ""
            elif value == "":
                default = 'default="", '
            else:
                default = f"default={value}, "
        return default

    def translate(self) -> ColumnInfoDict:
        comment = default = length = index = null = pk = ""
        if self.pk:
            pk = "primary_key=True, "
        else:
            if self.unique:
                index = "unique=True, "
            elif self.index:
                index = "db_index=True, "
            if self.default is not None:
                default = self.trans_default(self.default, self.data_type, self.extra)
        if self.data_type in ("varchar", "VARCHAR"):
            length = f"max_length={self.length}, "
        elif self.data_type in ("decimal", "numeric"):
            length_parts = []
            if self.max_digits:
                length_parts.append(f"max_digits={self.max_digits}")
            if self.decimal_places:
                length_parts.append(f"decimal_places={self.decimal_places}")
            if length_parts:
                length = ", ".join(length_parts) + ", "
        if self.null:
            null = "null=True, "
        if self.comment:
            comment = f"description='{self.comment}', "
        return {
            "name": self.name,
            "pk": pk,
            "index": index,
            "null": null,
            "default": default,
            "length": length,
            "comment": comment,
        }


class Inspect:
    _table_template = "class {table}(Model):\n"

    def __init__(
        self,
        conn: BaseDBAsyncClient,
        tables: list[str] | None = None,
        special_fields: dict[str, str] | None = None,
    ) -> None:
        self.conn = conn
        with contextlib.suppress(AttributeError):
            self.database = conn.database  # type:ignore[attr-defined]
        self.tables = tables
        self._special_fields = special_fields

    @property
    def field_map(self) -> FieldMapDict:
        raise NotImplementedError

    async def inspect(self) -> str:
        if not self.tables:
            self.tables = await self.get_all_tables()
        imports = ["from tortoise import Model, fields"]
        tables = []
        for table in self.tables:
            columns = await self.get_columns(table)
            fields = []
            model = self._table_template.format(table=table.title().replace("_", ""))
            for column in columns:
                try:
                    trans_func = self.field_map[column.data_type]
                except KeyError as e:
                    if not self._special_fields or column.data_type not in self._special_fields:
                        raise NotSupportError(
                            f"Can't translate {column.data_type=} to be tortoise field"
                        ) from e
                    field_class = self._special_fields[column.data_type]
                    is_normal_field = True
                    if "." in field_class:  # e.g.: tortoise.contrib.mysql.fields.GeometryField
                        module, field_class = field_class.rsplit(".", 1)
                        if module != "fields":
                            imports.append(f"from {module} import {field_class}")
                            is_normal_field = False
                    trans_func = partial(
                        self.get_field_string, field_class, is_normal_field=is_normal_field
                    )
                field = trans_func(**column.translate())
                fields.append("    " + field)
            tables.append(model + "\n".join(fields))
        result = "\n".join(imports) + "\n\n"
        return result + "\n\n\n".join(tables)

    async def get_columns(self, table: str) -> list[Column]:
        raise NotImplementedError

    async def get_all_tables(self) -> list[str]:
        raise NotImplementedError

    @staticmethod
    def get_field_string(
        field_class: str,
        arguments: str = "{null}{default}{comment}",
        is_normal_field: bool = True,
        **kwargs,
    ) -> str:
        name = kwargs["name"]
        field_params = arguments.format(**kwargs).strip().rstrip(",")
        if is_normal_field:
            field_class = "fields." + field_class
        return f"{name} = {field_class}({field_params})"

    @classmethod
    def decimal_field(cls, **kwargs) -> str:
        return cls.get_field_string("DecimalField", **kwargs)

    @classmethod
    def time_field(cls, **kwargs) -> str:
        return cls.get_field_string("TimeField", **kwargs)

    @classmethod
    def date_field(cls, **kwargs) -> str:
        return cls.get_field_string("DateField", **kwargs)

    @classmethod
    def float_field(cls, **kwargs) -> str:
        return cls.get_field_string("FloatField", **kwargs)

    @classmethod
    def datetime_field(cls, **kwargs) -> str:
        return cls.get_field_string("DatetimeField", **kwargs)

    @classmethod
    def text_field(cls, **kwargs) -> str:
        return cls.get_field_string("TextField", **kwargs)

    @classmethod
    def char_field(cls, **kwargs) -> str:
        arguments = "{pk}{index}{length}{null}{default}{comment}"
        return cls.get_field_string("CharField", arguments, **kwargs)

    @classmethod
    def int_field(cls, field_class="IntField", **kwargs) -> str:
        arguments = "{pk}{index}{default}{comment}"
        return cls.get_field_string(field_class, arguments, **kwargs)

    @classmethod
    def smallint_field(cls, **kwargs) -> str:
        return cls.int_field("SmallIntField", **kwargs)

    @classmethod
    def bigint_field(cls, **kwargs) -> str:
        return cls.int_field("BigIntField", **kwargs)

    @classmethod
    def bool_field(cls, **kwargs) -> str:
        return cls.get_field_string("BooleanField", **kwargs)

    @classmethod
    def uuid_field(cls, **kwargs) -> str:
        arguments = "{pk}{index}{default}{comment}"
        return cls.get_field_string("UUIDField", arguments, **kwargs)

    @classmethod
    def json_field(cls, **kwargs) -> str:
        return cls.get_field_string("JSONField", **kwargs)

    @classmethod
    def binary_field(cls, **kwargs) -> str:
        return cls.get_field_string("BinaryField", **kwargs)
