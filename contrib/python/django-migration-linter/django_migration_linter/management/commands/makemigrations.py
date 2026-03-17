from __future__ import annotations

import os

from django.conf import settings
from django.core.management import CommandParser
from django.core.management.commands.makemigrations import (
    Command as MakeMigrationsCommand,
)
from django.db.migrations import Migration
from django.db.migrations.questioner import InteractiveMigrationQuestioner
from django.db.migrations.writer import MigrationWriter

from django_migration_linter import MigrationLinter

from ..utils import (
    configure_logging,
    extract_warnings_as_errors_option,
    register_linting_configuration_options,
)


def ask_should_keep_migration() -> bool:
    questioner = InteractiveMigrationQuestioner()
    msg = """
The migration linter detected that this migration is not backward compatible.
- If you keep the migration, you will want to fix the issue or ignore the migration.
- By default, the newly created migration file will be deleted.
Do you want to keep the migration? [y/N]"""
    return questioner._boolean_input(msg, False)


def default_should_keep_migration():
    return False


class Command(MakeMigrationsCommand):
    help = "Creates new migration(s) for apps and lints them."

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "--lint",
            action="store_true",
            help="Lint newly generated migrations.",
        )
        register_linting_configuration_options(parser)

    def handle(self, *app_labels, **options):
        self.lint = options["lint"]
        self.database = options["database"]
        self.exclude_migrations_tests = options["exclude_migration_tests"]
        self.warnings_as_errors = options["warnings_as_errors"]
        self.sql_analyser = options["sql_analyser"]
        self.ignore_sqlmigrate_errors = options["ignore_sqlmigrate_errors"]
        configure_logging(options["verbosity"])
        return super().handle(*app_labels, **options)

    def write_migration_files(self, changes: dict[str, list[Migration]]) -> None:
        super().write_migration_files(changes)

        if (
            not getattr(settings, "MIGRATION_LINTER_OVERRIDE_MAKEMIGRATIONS", False)
            and not self.lint
        ):
            return

        if self.dry_run:
            """
            Since we rely on the 'sqlmigrate' to lint the migrations, we can only
            lint if the migration files have been generated. Since the 'dry-run'
            option won't generate the files, we cannot lint migrations.
            """
            return

        should_keep_migration = (
            ask_should_keep_migration
            if self.interactive
            else default_should_keep_migration
        )

        (
            warnings_as_errors_tests,
            all_warnings_as_errors,
        ) = extract_warnings_as_errors_option(self.warnings_as_errors)

        # Lint migrations.
        linter = MigrationLinter(
            path=os.environ["DJANGO_SETTINGS_MODULE"],
            database=self.database,
            no_cache=True,
            exclude_migration_tests=self.exclude_migrations_tests,
            warnings_as_errors_tests=warnings_as_errors_tests,
            all_warnings_as_errors=all_warnings_as_errors,
            analyser_string=self.sql_analyser,
            ignore_sqlmigrate_errors=self.ignore_sqlmigrate_errors,
        )

        for app_label, app_migrations in changes.items():
            if self.verbosity >= 1:
                self.stdout.write(
                    self.style.MIGRATE_HEADING("Linting for '%s':" % app_label) + "\n"
                )

            for migration in app_migrations:
                linter.lint_migration(migration)
                if linter.has_errors:
                    if not should_keep_migration():
                        self.delete_migration(migration)
                linter.reset_counters()

    def delete_migration(self, migration: Migration) -> None:
        writer = MigrationWriter(migration)
        os.remove(writer.path)

        if self.verbosity >= 1:
            try:
                migration_string = os.path.relpath(writer.path)
            except ValueError:
                migration_string = writer.path
            if migration_string.startswith(".."):
                migration_string = writer.path
            self.stdout.write(
                "Deleted %s\n" % (self.style.MIGRATE_LABEL(migration_string))
            )
