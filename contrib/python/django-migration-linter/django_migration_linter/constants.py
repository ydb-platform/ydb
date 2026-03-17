from __future__ import annotations

from appdirs import user_cache_dir

__version__ = "6.0.0"

DEFAULT_CACHE_PATH = user_cache_dir("django-migration-linter", version=__version__)

DJANGO_APPS_WITH_MIGRATIONS = ("admin", "auth", "contenttypes", "sessions")
EXPECTED_DATA_MIGRATION_ARGS = ("apps", "schema_editor")
