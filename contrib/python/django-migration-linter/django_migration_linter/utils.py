from __future__ import annotations

import os
from importlib import import_module


def split_path(path: str) -> list[str]:
    decomposed_path: list[str] = []
    while 1:
        head, tail = os.path.split(path)
        if head == path:  # sentinel for absolute paths
            decomposed_path.insert(0, head)
            break
        elif tail == path:  # sentinel for relative paths
            decomposed_path.insert(0, tail)
            break
        else:
            path = head
            decomposed_path.insert(0, tail)

    if not decomposed_path[-1]:
        decomposed_path = decomposed_path[:-1]
    return decomposed_path


def split_migration_path(migration_path: str) -> tuple[str, str]:
    from django.db.migrations.loader import MIGRATIONS_MODULE_NAME

    decomposed_path = split_path(migration_path)
    for i, p in enumerate(decomposed_path):
        if p == MIGRATIONS_MODULE_NAME:
            return decomposed_path[i - 1], os.path.splitext(decomposed_path[i + 1])[0]
    return "", ""


def clean_bytes_to_str(byte_input: bytes) -> str:
    return byte_input.decode("utf-8").strip()


def get_migration_abspath(app_label: str, migration_name: str) -> str:
    from django.db.migrations.loader import MigrationLoader

    module_name, _ = MigrationLoader.migrations_module(app_label)
    migration_path = f"{module_name}.{migration_name}"
    migration_module = import_module(migration_path)

    migration_file = migration_module.__file__
    if not migration_file:
        raise ValueError("Migration file not found")

    if migration_file.endswith(".pyc"):
        migration_file = migration_file[:-1]
    return migration_file
