# mypy: disable-error-code="no-redef"
from __future__ import annotations

import platform
import re
import sys
from types import ModuleType
from typing import TYPE_CHECKING, cast

import tortoise

if sys.version_info >= (3, 11):
    from typing import Self

    import tomllib
else:
    from typing_extensions import Self

    try:
        import tomli as tomllib
    except ImportError:
        import tomlkit as tomllib

if TYPE_CHECKING:
    from tortoise import Model
    from tortoise.fields.relational import ManyToManyFieldInstance


__all__ = ("Self", "tomllib", "imports_tomlkit", "tortoise_version_less_than")


def imports_tomlkit() -> ModuleType:
    try:
        import tomli_w as tomlkit
    except ImportError:
        import tomlkit
    return tomlkit


def tortoise_version_less_than(version: str) -> bool:
    # The min version of tortoise is '0.11.0', so we can compare it by a `<`,
    return tortoise.__version__ < version


def _init_asyncio_patch() -> None:
    """
    Select compatible event loop for psycopg3.

    As of Python 3.8+, the default event loop on Windows is `proactor`,
    however psycopg3 requires the old default "selector" event loop.
    See https://www.psycopg.org/psycopg3/docs/advanced/async.html
    """
    if platform.system() == "Windows":
        try:
            from asyncio import WindowsSelectorEventLoopPolicy  # type:ignore
        except ImportError:
            pass  # Can't assign a policy which doesn't exist.
        else:
            from asyncio import get_event_loop_policy, set_event_loop_policy

            if not isinstance(get_event_loop_policy(), WindowsSelectorEventLoopPolicy):
                set_event_loop_policy(WindowsSelectorEventLoopPolicy())


def _init_tortoise_0_24_1_patch() -> None:
    # this patch is for "tortoise-orm==0.24.1" to fix:
    # https://github.com/tortoise/tortoise-orm/issues/1893
    if tortoise.__version__ != "0.24.1":
        return
    from tortoise.backends.base.schema_generator import BaseSchemaGenerator

    target_func = "_get_m2m_tables"

    def _get_m2m_tables(  # Copied from tortoise-orm 0.25
        self: BaseSchemaGenerator,
        model: type[Model],
        db_table: str,
        safe: bool,
        models_tables: list[str],
    ) -> list[str]:
        m2m_tables_for_create = []
        for m2m_field in model._meta.m2m_fields:
            field_object = cast("ManyToManyFieldInstance", model._meta.fields_map[m2m_field])
            if field_object._generated or field_object.through in models_tables:
                continue
            backward_key, forward_key = field_object.backward_key, field_object.forward_key
            if field_object.db_constraint:
                backward_fk = self._create_fk_string(
                    "",
                    backward_key,
                    db_table,
                    model._meta.db_pk_column,
                    field_object.on_delete,
                    "",
                )
                forward_fk = self._create_fk_string(
                    "",
                    forward_key,
                    field_object.related_model._meta.db_table,
                    field_object.related_model._meta.db_pk_column,
                    field_object.on_delete,
                    "",
                )
            else:
                backward_fk = forward_fk = ""
            exists = "IF NOT EXISTS " if safe else ""
            through_table_name = field_object.through
            backward_type = forward_type = comment = ""
            if func := getattr(self, "_get_pk_field_sql_type", None):
                backward_type = func(model._meta.pk)
                forward_type = func(field_object.related_model._meta.pk)
            if desc := field_object.description:
                comment = self._table_comment_generator(table=through_table_name, comment=desc)
            m2m_create_string = self.M2M_TABLE_TEMPLATE.format(
                exists=exists,
                table_name=through_table_name,
                backward_fk=backward_fk,
                forward_fk=forward_fk,
                backward_key=backward_key,
                backward_type=backward_type,
                forward_key=forward_key,
                forward_type=forward_type,
                extra=self._table_generate_extra(table=field_object.through),
                comment=comment,
            )
            if not field_object.db_constraint:
                m2m_create_string = m2m_create_string.replace(
                    """,
    ,
    """,
                    "",
                )  # may have better way
            m2m_create_string += self._post_table_hook()
            if getattr(field_object, "create_unique_index", field_object.unique):
                unique_index_create_sql = self._get_unique_index_sql(
                    exists, through_table_name, [backward_key, forward_key]
                )
                if unique_index_create_sql.endswith(";"):
                    m2m_create_string += "\n" + unique_index_create_sql
                else:
                    lines = m2m_create_string.splitlines()
                    lines[-2] += ","
                    indent = m.group() if (m := re.match(r"\s+", lines[-2])) else ""
                    lines.insert(-1, indent + unique_index_create_sql)
                    m2m_create_string = "\n".join(lines)
            m2m_tables_for_create.append(m2m_create_string)
        return m2m_tables_for_create

    setattr(BaseSchemaGenerator, target_func, _get_m2m_tables)
