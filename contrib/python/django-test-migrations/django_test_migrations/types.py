from typing import Any, TypeAlias, Union

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.migrations import Migration
from django.utils.connection import ConnectionProxy

# Migration target: (app_name, migration_name)
# Regular or rollback migration: 0001 -> 0002, or 0002 -> 0001
# Rollback migration to initial state: 0001 -> None
MigrationTarget: TypeAlias = tuple[str, str | None]
MigrationSpec: TypeAlias = MigrationTarget | list[MigrationTarget]

MigrationPlan: TypeAlias = list[tuple[Migration, bool]]

AnyConnection: TypeAlias = Union['ConnectionProxy[Any]', BaseDatabaseWrapper]

DatabaseSettingValue: TypeAlias = str | int
