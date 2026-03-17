from __future__ import annotations

from django.db.migrations.operations.base import Operation


class IgnoreMigration(Operation):
    """
    No-op migration operation that will enable the Django Migration Linter
    to detect if the entire migration should be ignored (through code).
    """

    reversible = True
    reduces_to_sql = False
    elidable = True

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        pass

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        pass

    def describe(self):
        return "The Django migration linter will ignore this migration"
