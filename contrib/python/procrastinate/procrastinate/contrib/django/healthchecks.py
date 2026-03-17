from __future__ import annotations

from asgiref import sync
from django.core import exceptions as django_exceptions
from django.db import connections
from django.db import utils as db_utils
from django.db.migrations import executor as executor_module

from procrastinate import app as procrastinate_app
from procrastinate import exceptions

from . import app as django_app
from . import settings


@sync.sync_to_async
def healthchecks(app: procrastinate_app.App):
    alias = settings.settings.DATABASE_ALIAS
    connection = connections[alias]
    try:
        connection.ensure_connection()
    except db_utils.DatabaseError as exc:
        raise exceptions.ConnectorException() from exc

    print("Database connection: OK")

    try:
        executor = executor_module.MigrationExecutor(connections[alias])
    except django_exceptions.ImproperlyConfigured as exc:
        raise exceptions.ConnectorException() from exc

    plan = executor.migration_plan(executor.loader.graph.leaf_nodes())
    missing_migration = "procrastinate" in {
        migration.app_label for migration, _ in plan
    }
    if missing_migration:
        raise exceptions.ConnectorException(
            "Missing procrastinate migrations. Run `python manage.py migrate procrastinate`."
        )
    print("Migrations: OK")

    # Run a query the way Procrastinate does to ensure the connection is working
    django_app.check_connection()
    print("Default Django Procrastinate App: OK")

    # Same, with the worker app
    app.check_connection()
    print("Worker App: OK")
