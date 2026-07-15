from collections.abc import Callable
from typing import Any

from alembic.operations.ops import MigrationScript
from alembic.runtime.migration import MigrationContext

from clickhouse_connect.cc_sqlalchemy.alembic.adapter import (
    include_object as base_include_object,
)


def make_include_name(
    include_schemas: frozenset[str] | None = None, exclude_mv_pattern: str = "_mv", default_schema: str = "default"
) -> Callable:
    """Factory for include_name callback"""

    def include_name_callback(name: str | None, type_: str, parent_names: dict) -> bool:
        if type_ == "schema":
            schema_name = name if name else default_schema
            if include_schemas is not None:
                return schema_name in include_schemas
            return True

        if type_ == "table":
            if isinstance(name, str) and name.endswith(exclude_mv_pattern):
                return False
            schema = parent_names.get("schema_name") or default_schema
            if include_schemas is not None:
                return schema in include_schemas
            return True

        return True

    return include_name_callback


def make_include_object(
    exclude_tables: frozenset[str] | None = None,
    include_schemas: frozenset[str] | None = None,
    exclude_mv_pattern: str = "_mv",
    base_include_object_fn: Callable | None = None,
) -> Callable:
    """Factory for include_object callback"""

    def include_object_callback(object_: Any, name: str | None, type_: str, reflected: bool, compare_to: Any) -> bool:
        if base_include_object_fn and not base_include_object_fn(object_, name, type_, reflected, compare_to):
            return False

        if not base_include_object(object_, name, type_, reflected, compare_to):
            return False

        if type_ == "table":
            if include_schemas and object_.schema not in include_schemas:
                return False

            if isinstance(name, str) and name.endswith(exclude_mv_pattern):
                return False

            if exclude_tables:
                fullname = f"{object_.schema}.{name}" if object_.schema else name
                if fullname in exclude_tables:
                    return False
                if name in exclude_tables:
                    return False

        return True

    return include_object_callback


def prevent_empty_migrations(writer_fn: Callable) -> Callable:
    """Wrapper to prevent empty migration generation"""

    def wrapper(context: MigrationContext, revision: Any, directives: list[MigrationScript]) -> None:
        if not directives:
            return
        config = context.config
        if getattr(config.cmd_opts, "autogenerate", False):
            script = directives[0]
            if script.upgrade_ops.is_empty():
                directives.clear()
                return
        writer_fn(context, revision, directives)

    return wrapper
