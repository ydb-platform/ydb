from __future__ import annotations

import functools
import hashlib
import inspect
import logging
import os
import re
from enum import Enum, unique
from importlib.util import find_spec
from subprocess import PIPE, Popen
from typing import Callable, Dict, Iterable

from django.conf import settings
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS, ProgrammingError, connections
from django.db.migrations import Migration, RunPython, RunSQL
from django.db.migrations.operations.base import Operation

from .cache import Cache
from .constants import (
    DEFAULT_CACHE_PATH,
    DJANGO_APPS_WITH_MIGRATIONS,
    EXPECTED_DATA_MIGRATION_ARGS,
)
from .operations import IgnoreMigration
from .sql_analyser import analyse_sql_statements, get_sql_analyser_class
from .sql_analyser.base import Issue
from .utils import clean_bytes_to_str, get_migration_abspath, split_migration_path

logger = logging.getLogger("django_migration_linter")


@unique
class MessageType(Enum):
    OK = "ok"
    IGNORE = "ignore"
    WARNING = "warning"
    ERROR = "error"

    @staticmethod
    def values() -> list[str]:
        return list(map(lambda c: c.value, MessageType))


class MigrationLinter:
    def __init__(
        self,
        path: str | None = None,
        ignore_name_contains: str | None = None,
        ignore_name: Iterable[str] | None = None,
        include_name_contains: str | None = None,
        include_name: Iterable[str] | None = None,
        include_apps: Iterable[str] | None = None,
        exclude_apps: Iterable[str] | None = None,
        database: str = DEFAULT_DB_ALIAS,
        cache_path: str = DEFAULT_CACHE_PATH,
        no_cache: bool = False,
        only_applied_migrations: bool = False,
        only_unapplied_migrations: bool = False,
        exclude_migration_tests: Iterable[str] | None = None,
        quiet: Iterable[str] | None = None,
        warnings_as_errors_tests: Iterable[str] | None = None,
        all_warnings_as_errors: bool = False,
        no_output: bool = False,
        analyser_string: str | None = None,
        ignore_sqlmigrate_errors: bool = False,
        ignore_initial_migrations: bool = False,
    ):
        # Store parameters and options
        self.django_path = path
        self.ignore_name_contains = ignore_name_contains
        self.ignore_name = ignore_name or []
        self.include_name_contains = include_name_contains
        self.include_name = include_name or []
        self.include_apps = include_apps
        self.exclude_apps = exclude_apps
        self.exclude_migration_tests = exclude_migration_tests or []
        self.database = database or DEFAULT_DB_ALIAS
        self.cache_path = cache_path or DEFAULT_CACHE_PATH
        self.no_cache = no_cache
        self.only_applied_migrations = only_applied_migrations
        self.only_unapplied_migrations = only_unapplied_migrations
        self.quiet = quiet or []
        self.warnings_as_errors_tests = warnings_as_errors_tests
        self.all_warnings_as_errors = all_warnings_as_errors
        self.no_output = no_output
        self.sql_analyser_class = get_sql_analyser_class(
            settings.DATABASES[self.database]["ENGINE"],
            analyser_string=analyser_string,
        )
        self.ignore_sqlmigrate_errors = ignore_sqlmigrate_errors
        self.ignore_initial_migrations = ignore_initial_migrations

        # Initialise counters
        self.reset_counters()

        # Initialise cache. Read from old, write to new, in order to prune old entries.
        if self.should_use_cache():
            self.old_cache = Cache(self.django_path, self.database, self.cache_path)
            self.new_cache = Cache(self.django_path, self.database, self.cache_path)
            self.old_cache.load()

        # Initialise migrations
        from django.db.migrations.loader import MigrationLoader

        self.migration_loader = MigrationLoader(
            connection=connections[self.database], load=True
        )

    def reset_counters(self) -> None:
        self.nb_valid = 0
        self.nb_ignored = 0
        self.nb_warnings = 0
        self.nb_erroneous = 0
        self.nb_total = 0

    def should_use_cache(self) -> bool:
        return bool(self.django_path and not self.no_cache)

    def lint_all_migrations(
        self,
        app_label: str | None = None,
        migration_name: str | None = None,
        git_commit_id: str | None = None,
        migrations_file_path: str | None = None,
    ) -> None:
        # Collect migrations.
        migrations_list = self.read_migrations_list(migrations_file_path)
        if git_commit_id:
            migrations = self._gather_migrations_git(git_commit_id, migrations_list)
        else:
            migrations = self._gather_all_migrations(migrations_list)

        # Lint those migrations.
        sorted_migrations = sorted(
            migrations, key=lambda migration: (migration.app_label, migration.name)
        )

        specific_target_migration = (
            self.migration_loader.get_migration_by_prefix(app_label, migration_name)
            if app_label and migration_name
            else None
        )

        for m in sorted_migrations:
            if app_label and migration_name:
                if m == specific_target_migration:
                    self.lint_migration(m)
            elif app_label:
                if m.app_label == app_label:
                    self.lint_migration(m)
            else:
                self.lint_migration(m)

        if self.should_use_cache():
            self.new_cache.save()

    def lint_migration(self, migration: Migration) -> None:
        app_label = migration.app_label
        migration_name = migration.name
        operations = migration.operations
        self.nb_total += 1

        md5hash = self.get_migration_hash(app_label, migration_name)

        if self.should_ignore_migration(
            app_label, migration_name, operations, is_initial=migration.initial
        ):
            self.print_linting_msg(
                app_label, migration_name, "IGNORE", MessageType.IGNORE
            )
            self.nb_ignored += 1
            return

        if self.should_use_cache() and md5hash in self.old_cache:
            self.lint_cached_migration(app_label, migration_name, md5hash)
            return

        sql_statements = self.get_sql(app_label, migration_name)
        errors, ignored, warnings = analyse_sql_statements(
            self.sql_analyser_class,
            sql_statements,
            self.exclude_migration_tests,
        )

        err, ignored_data, warnings_data = self.analyse_data_migration(migration)
        if err:
            errors += err
        if ignored_data:
            ignored += ignored_data
        if warnings_data:
            warnings += warnings_data

        if self.all_warnings_as_errors:
            errors += warnings
            warnings = []
        elif self.warnings_as_errors_tests:
            new_warnings = []
            for w in warnings:
                if w.code in self.warnings_as_errors_tests:
                    errors.append(w)
                else:
                    new_warnings.append(w)
            warnings = new_warnings

        # Fixme: have a more generic approach to handling errors/warnings/ignored/ok?
        if errors:
            self.print_linting_msg(app_label, migration_name, "ERR", MessageType.ERROR)
            self.nb_erroneous += 1
            self.print_errors(errors)
            if warnings:
                self.print_warnings(warnings)
            value_to_cache = {"result": "ERR", "errors": errors, "warnings": warnings}
        elif warnings:
            self.print_linting_msg(
                app_label, migration_name, "WARNING", MessageType.WARNING
            )
            self.nb_warnings += 1
            self.print_warnings(warnings)
            value_to_cache = {"result": "WARNING", "warnings": warnings}
            # Fixme: not displaying ignored errors, when
        else:
            if ignored:
                self.print_linting_msg(
                    app_label, migration_name, "OK (ignored)", MessageType.IGNORE
                )
                self.print_errors(ignored)
            else:
                self.print_linting_msg(app_label, migration_name, "OK", MessageType.OK)
            self.nb_valid += 1
            value_to_cache = {"result": "OK"}

        if self.should_use_cache():
            self.new_cache[md5hash] = value_to_cache

    @staticmethod
    def get_migration_hash(app_label: str, migration_name: str) -> str:
        hash_md5 = hashlib.md5(usedforsecurity=False)
        with open(get_migration_abspath(app_label, migration_name), "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def lint_cached_migration(
        self, app_label: str, migration_name: str, md5hash: str
    ) -> None:
        cached_value = self.old_cache[md5hash]
        if cached_value["result"] == "IGNORE":
            self.print_linting_msg(
                app_label, migration_name, "IGNORE (cached)", MessageType.IGNORE
            )
            self.nb_ignored += 1
        elif cached_value["result"] == "OK":
            self.print_linting_msg(
                app_label, migration_name, "OK (cached)", MessageType.OK
            )
            self.nb_valid += 1
        elif cached_value["result"] == "WARNING":
            self.print_linting_msg(
                app_label, migration_name, "WARNING (cached)", MessageType.WARNING
            )
            self.nb_warnings += 1
            self.print_warnings(cached_value["warnings"])
        else:
            self.print_linting_msg(
                app_label, migration_name, "ERR (cached)", MessageType.ERROR
            )
            self.nb_erroneous += 1
            if "errors" in cached_value:
                self.print_errors(cached_value["errors"])
            if "warnings" in cached_value and cached_value["warnings"]:
                self.print_warnings(cached_value["warnings"])

        self.new_cache[md5hash] = cached_value

    def print_linting_msg(
        self, app_label: str, migration_name: str, msg: str, lint_result: MessageType
    ) -> None:
        if lint_result.value in self.quiet:
            return
        if not self.no_output:
            print(f"({app_label}, {migration_name})... {msg}")

    def print_errors(self, errors: list[Issue]) -> None:
        if MessageType.ERROR.value in self.quiet:
            return
        for err in errors:
            error_str = "\t{}".format(err.message)
            if err.table:
                error_str += " (table: {}".format(err.table)
                if err.column:
                    error_str += ", column: {}".format(err.column)
                error_str += ")"
            if not self.no_output:
                print(error_str)

    def print_warnings(self, warnings: list[Issue]) -> None:
        if MessageType.WARNING.value in self.quiet:
            return

        for warning_details in warnings:
            warn_str = "\t{}".format(warning_details.message)
            if not self.no_output:
                print(warn_str)

    def print_summary(self) -> None:
        if self.no_output:
            return
        print("*** Summary ***")
        print(f"Valid migrations: {self.nb_valid}/{self.nb_total}")
        print(f"Erroneous migrations: {self.nb_erroneous}/{self.nb_total}")
        print(f"Migrations with warnings: {self.nb_warnings}/{self.nb_total}")
        print(f"Ignored migrations: {self.nb_ignored}/{self.nb_total}")

    @property
    def has_errors(self) -> bool:
        return self.nb_erroneous > 0

    def get_sql(self, app_label: str, migration_name: str) -> list[str]:
        logger.info(f"Calling sqlmigrate command {app_label} {migration_name}")
        try:
            with open(os.devnull, "w") as dev_null:
                sql_statement = call_command(
                    "sqlmigrate",
                    app_label,
                    migration_name,
                    database=self.database,
                    stdout=dev_null,
                )
        except (ValueError, ProgrammingError) as err:
            if self.ignore_sqlmigrate_errors:
                logger.warning(
                    "Error while executing sqlmigrate on (%s, %s) with exception: %s. "
                    "Continuing execution with empty SQL.",
                    app_label,
                    migration_name,
                    str(err),
                )
                sql_statement = ""
            else:
                logger.warning(
                    "Error while executing sqlmigrate on (%s, %s) with exception: %s.",
                    app_label,
                    migration_name,
                    str(err),
                )
                raise
        return sql_statement.splitlines()

    @staticmethod
    def is_migration_file(filename: str) -> bool:
        from django.db.migrations.loader import MIGRATIONS_MODULE_NAME

        return bool(
            re.search(rf"/{MIGRATIONS_MODULE_NAME}/.*\.py", filename)
            and "__init__" not in filename
        )

    @classmethod
    def read_migrations_list(
        cls, migrations_file_path: str | None
    ) -> list[tuple[str, str]] | None:
        """
        Returning an empty list is different from returning None here.
        None: no file was specified and we should consider all migrations
        Empty list: no migration found in the file and we should consider no migration
        """
        if not migrations_file_path:
            return None

        migrations = []
        try:
            with open(migrations_file_path) as file:
                for line in file:
                    if cls.is_migration_file(line):
                        app_label, name = split_migration_path(line)
                        migrations.append((app_label, name))
        except OSError:
            logger.exception("Migrations list path not found %s", migrations_file_path)
            raise Exception("Error while reading migrations list file")

        if not migrations:
            logger.info(
                "No valid migration paths found in the migrations file %s",
                migrations_file_path,
            )
        return migrations

    def _gather_migrations_git(
        self, git_commit_id: str, migrations_list: list[tuple[str, str]] | None = None
    ) -> Iterable[Migration]:
        # Get changes since specified commit
        git_diff_command = [
            "git",
            "diff",
            "--relative",
            "--name-only",
            "--diff-filter=AR",
            git_commit_id,
        ]
        logger.info(f"Executing {git_diff_command} (in {self.django_path})")
        diff_process = Popen(
            git_diff_command,
            stdout=PIPE,
            stderr=PIPE,
            cwd=self.django_path,
        )

        diskpath_and_migration: Dict[str, Migration] = {}
        for migration in self._gather_all_migrations():
            spec = find_spec(migration.__module__)
            if spec:
                diskpath_and_migration[str(spec.origin)] = migration

        migrations = []
        for line in map(
            clean_bytes_to_str,
            diff_process.stdout.readlines(),  # type: ignore
        ):
            # Only gather lines that include added migrations.
            if self.is_migration_file(line):
                # Find the migration objects with similar path.
                suitable_migrations = [
                    migration
                    for path, migration in diskpath_and_migration.items()
                    if path.endswith(line)
                ]
                if suitable_migrations:
                    migration = suitable_migrations[0]
                    if (
                        migrations_list is None
                        or (migration.app_label, migration.name) in migrations_list
                    ):
                        migrations.append(migration)
                    if len(suitable_migrations) > 1:
                        # If multiple migration founds, we chose one randomly, but need
                        # to alert that it's not very precise.
                        logger.warning(
                            "Found multiple migration files matching altered "
                            "file path %s. Choose (%s, %s) opportunistically.",
                            line,
                            migration.app_label,
                            migration.name,
                        )
                else:
                    app_label, name = split_migration_path(line)
                    logger.info(
                        "Found migration file (%s, %s) "
                        "that is not present in loaded migration.",
                        app_label,
                        name,
                    )
        diff_process.wait()

        if diff_process.returncode != 0:
            output = []
            for line in map(
                clean_bytes_to_str,
                diff_process.stderr.readlines(),  # type: ignore
            ):
                output.append(line)
            logger.error("Error while git diff command:\n{}".format("".join(output)))
            raise Exception("Error while executing git diff command")
        return migrations

    def _gather_all_migrations(
        self, migrations_list: list[tuple[str, str]] | None = None
    ) -> Iterable[Migration]:
        for (
            (app_label, name),
            migration,
        ) in self.migration_loader.disk_migrations.items():
            if app_label not in DJANGO_APPS_WITH_MIGRATIONS:  # Prune Django apps
                if migrations_list is None or (app_label, name) in migrations_list:
                    yield migration

    def should_ignore_migration(
        self,
        app_label: str,
        migration_name: str,
        operations: Iterable[Operation] = (),
        is_initial: bool = False,
    ) -> bool:
        return (
            (self.include_apps and app_label not in self.include_apps)
            or (self.exclude_apps and app_label in self.exclude_apps)
            or any(isinstance(o, IgnoreMigration) for o in operations)
            or (
                self.ignore_name_contains
                and self.ignore_name_contains in migration_name
            )
            or (
                self.include_name_contains
                and self.include_name_contains not in migration_name
            )
            or (migration_name in self.ignore_name)
            or (self.include_name and migration_name not in self.include_name)
            or (
                self.only_applied_migrations
                and (app_label, migration_name)
                not in self.migration_loader.applied_migrations
            )
            or (
                self.only_unapplied_migrations
                and (app_label, migration_name)
                in self.migration_loader.applied_migrations
            )
            or (self.ignore_initial_migrations and is_initial)
        )

    def analyse_data_migration(
        self, migration: Migration
    ) -> tuple[list[Issue], list[Issue], list[Issue]]:
        errors = []
        ignored = []
        warnings = []

        for operation in migration.operations:
            if isinstance(operation, RunPython):
                op_errors, op_ignored, op_warnings = self.lint_runpython(operation)
            elif isinstance(operation, RunSQL):
                op_errors, op_ignored, op_warnings = self.lint_runsql(operation)
            else:
                op_errors, op_ignored, op_warnings = [], [], []

            if op_errors:
                errors += op_errors
            if op_ignored:
                ignored += op_ignored
            if op_warnings:
                warnings += op_warnings

        return errors, ignored, warnings

    @staticmethod
    def discover_function(function):
        if isinstance(function, functools.partial):
            return function.func
        return function

    def lint_runpython(
        self, runpython: RunPython
    ) -> tuple[list[Issue], list[Issue], list[Issue]]:
        function_name = self.discover_function(runpython.code).__name__
        error = []
        ignored = []
        warning = []

        # Detect warning on missing reverse operation
        if not runpython.reversible:
            issue = Issue(
                code="RUNPYTHON_REVERSIBLE",
                message="'{}': RunPython data migration is not reversible".format(
                    function_name
                ),
            )
            if issue.code in self.exclude_migration_tests:
                ignored.append(issue)
            else:
                warning.append(issue)

        # Detect warning for argument naming convention
        args_spec = inspect.getfullargspec(runpython.code)
        if tuple(args_spec.args) != EXPECTED_DATA_MIGRATION_ARGS:
            issue = Issue(
                code="RUNPYTHON_ARGS_NAMING_CONVENTION",
                message=(
                    "'{}': By convention, "
                    "RunPython names the two arguments: apps, schema_editor"
                ).format(function_name),
            )
            if issue.code in self.exclude_migration_tests:
                ignored.append(issue)
            else:
                warning.append(issue)

        # Detect wrong model imports
        # Forward
        issues = self.get_runpython_model_import_issues(runpython.code)
        for issue in issues:
            if issue.code in self.exclude_migration_tests:
                ignored.append(issue)
            else:
                error.append(issue)

        # Backward
        if runpython.reversible:
            issues = self.get_runpython_model_import_issues(runpython.reverse_code)
            for issue in issues:
                if issue and issue.code in self.exclude_migration_tests:
                    ignored.append(issue)
                else:
                    error.append(issue)

        # Detect warning if model variable name is not the same as model class
        issues = self.get_runpython_model_variable_naming_issues(runpython.code)
        for issue in issues:
            if issue and issue.code in self.exclude_migration_tests:
                ignored.append(issue)
            else:
                warning.append(issue)

        if runpython.reversible:
            issues = self.get_runpython_model_variable_naming_issues(
                runpython.reverse_code
            )
            for issue in issues:
                if issue and issue.code in self.exclude_migration_tests:
                    ignored.append(issue)
                else:
                    warning.append(issue)

        return error, ignored, warning

    @staticmethod
    def get_runpython_model_import_issues(code: Callable) -> list[Issue]:
        model_object_regex = re.compile(r"[^a-zA-Z0-9._]?([a-zA-Z0-9._]+?)\.objects")

        function = MigrationLinter.discover_function(code)
        function_name = function.__name__
        source_code = inspect.getsource(function)

        called_models = model_object_regex.findall(source_code)
        issues = []
        for model in called_models:
            model = model.split(".", 1)[0]
            has_get_model_call = (
                re.search(
                    rf"{model}.*= +\w+\.get_model\(",
                    source_code,
                )
                is not None
            )
            if not has_get_model_call:
                issues.append(
                    Issue(
                        code="RUNPYTHON_MODEL_IMPORT",
                        message=(
                            "'{}': Could not find an 'apps.get_model(\"...\", \"{}\")' "
                            "call. Importing the model directly is incorrect for "
                            "data migrations."
                        ).format(function_name, model),
                    )
                )
        return issues

    @staticmethod
    def get_runpython_model_variable_naming_issues(code: Callable) -> list[Issue]:
        model_object_regex = re.compile(r"[^a-zA-Z]?([a-zA-Z0-9]+?)\.objects")

        function = MigrationLinter.discover_function(code)
        function_name = function.__name__
        source_code = inspect.getsource(function)

        called_models = model_object_regex.findall(source_code)
        issues = []
        for model in called_models:
            has_same_model_name = (
                re.search(
                    r"{model}.*= +\w+?\.get_model\([^)]+?\.{model}.*?\)".format(
                        model=model
                    ),
                    source_code,
                    re.MULTILINE | re.DOTALL,
                )
                is not None
                or re.search(
                    r"{model}.*= +\w+?\.get_model\([^)]+?,[^)]*?{model}.*?\)".format(
                        model=model
                    ),
                    source_code,
                    re.MULTILINE | re.DOTALL,
                )
                is not None
            )
            if not has_same_model_name:
                issues.append(
                    Issue(
                        code="RUNPYTHON_MODEL_VARIABLE_NAME",
                        message=(
                            "'{}': Model variable name {} is different from the "
                            "model class name that was found in the "
                            "apps.get_model(...) call."
                        ).format(function_name, model),
                    )
                )
        return issues

    def lint_runsql(
        self, runsql: RunSQL
    ) -> tuple[list[Issue], list[Issue], list[Issue]]:
        error = []
        ignored = []
        warning = []

        # Detect warning on missing reverse operation
        if not runsql.reversible:
            issue = Issue(
                code="RUNSQL_REVERSIBLE",
                message="RunSQL data migration is not reversible",
            )
            if issue.code in self.exclude_migration_tests:
                ignored.append(issue)
            else:
                warning.append(issue)

        # Put the SQL in our SQL analyser
        if runsql.sql != RunSQL.noop:
            sql_statements = []
            if isinstance(runsql.sql, (list, tuple)):
                for sql in runsql.sql:
                    params = None
                    if isinstance(sql, (list, tuple)):
                        elements = len(sql)
                        if elements == 2:
                            sql, params = sql
                        else:
                            raise ValueError("Expected a 2-tuple but got %d" % elements)
                        sql_statements.append(sql % params)
                    else:
                        sql_statements.append(sql)
            else:
                sql_statements.append(runsql.sql)

            sql_errors, sql_ignored, sql_warnings = analyse_sql_statements(
                self.sql_analyser_class,
                sql_statements,
                self.exclude_migration_tests,
            )
            if sql_errors:
                error += sql_errors
            if sql_ignored:
                ignored += sql_ignored
            if sql_warnings:
                warning += sql_warnings

        # And analysse the reverse SQL
        if runsql.reversible and runsql.reverse_sql != RunSQL.noop:
            sql_statements = []
            if isinstance(runsql.reverse_sql, (list, tuple)):
                for sql in runsql.reverse_sql:
                    params = None
                    if isinstance(sql, (list, tuple)):
                        elements = len(sql)
                        if elements == 2:
                            sql, params = sql
                        else:
                            raise ValueError("Expected a 2-tuple but got %d" % elements)
                        sql_statements.append(sql % params)
                    else:
                        sql_statements.append(sql)
            else:
                sql_statements.append(runsql.reverse_sql)

            sql_errors, sql_ignored, sql_warnings = analyse_sql_statements(
                self.sql_analyser_class,
                sql_statements,
                self.exclude_migration_tests,
            )
            if sql_errors:
                error += sql_errors
            if sql_ignored:
                ignored += sql_ignored
            if sql_warnings:
                warning += sql_warnings

        return error, ignored, warning
