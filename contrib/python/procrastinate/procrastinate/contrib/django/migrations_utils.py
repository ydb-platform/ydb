from __future__ import annotations

import functools
from importlib import resources

from django.db import migrations


@functools.cache
def list_migration_files() -> dict[str, str]:
    """
    Returns a list of filenames and file contents for all migration files
    """
    return {
        p.name: p.read_text(encoding="utf-8")
        for p in resources.files("procrastinate.sql.migrations").iterdir()
        if p.name.endswith(".sql")
    }


class RunProcrastinateSQL(migrations.RunSQL):
    """
    A RunSQL migration that reads the SQL from the procrastinate.sql.migrations package
    """

    def __init__(self, name: str):
        self.name = name
        super().__init__(sql=list_migration_files()[name])
