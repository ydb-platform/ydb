from __future__ import annotations

import logging
import re
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable

logger = logging.getLogger("django_migration_linter")


def find_check_from_code(checks: list[Check], code: str) -> Check | None:
    return next((c for c in checks if c.code == code), None)


def update_migration_checks(
    base_checks: list[Check], specific_checks: list[Check]
) -> list[Check]:
    new_checks = deepcopy(base_checks)
    for override_check in specific_checks:
        migration_check = find_check_from_code(new_checks, override_check.code)

        if migration_check is None or not override_check.code:
            migration_check = deepcopy(override_check)
            new_checks.append(migration_check)

        for field in override_check.__dataclass_fields__:
            setattr(migration_check, field, getattr(override_check, field))
    return new_checks


def has_not_null_column(sql_statements: list[str], **kwargs) -> bool:
    # TODO: improve to detect that the same column is concerned
    not_null_column = False
    has_default_value = False

    for sql in sql_statements:
        if re.search("(?<!DROP )(?<!IS )NOT NULL", sql) and not (
            sql.startswith("CREATE TABLE")
            or sql.startswith("CREATE INDEX")
            or sql.startswith("CREATE UNIQUE INDEX")
        ):
            not_null_column = True
        if re.search("DEFAULT (?!NULL).*NOT NULL", sql):
            has_default_value = True
        if "SET DEFAULT" in sql and "SET DEFAULT NULL" not in sql:
            has_default_value = True
        if "DROP DEFAULT" in sql:
            has_default_value = False

    return not_null_column and not has_default_value


def has_add_unique(sql_statements: list[str], **kwargs) -> bool:
    regex_result = None
    for sql in sql_statements:
        regex_result = re.search(
            "ALTER TABLE (.*) ADD CONSTRAINT .* UNIQUE", sql
        ) or re.search('CREATE UNIQUE INDEX .* ON (".*?")', sql)
        if regex_result:
            break
    if not regex_result:
        return False

    concerned_table = regex_result.group(1)
    table_is_added_in_transaction = any(
        sql.startswith(f"CREATE TABLE {concerned_table}") for sql in sql_statements
    )
    return not table_is_added_in_transaction


class CheckMode(Enum):
    """
    Defines whether the Check.fn gets a str or a list[str] as first parameter.
    """

    ONE_LINER = 1
    TRANSACTION = 2


class CheckType(Enum):
    ERROR = 1
    WARNING = 2


@dataclass
class Check:
    """
    Represents a check that will be done against SQL statement(s).
    """

    code: str
    fn: Callable
    message: str
    mode: CheckMode
    type: CheckType


@dataclass
class Issue:
    code: str
    message: str
    table: str | None = None
    column: str | None = None


class BaseAnalyser:
    base_migration_checks: list[Check] = [
        Check(
            code="RENAME_TABLE",
            fn=lambda sql, **kw: re.search("RENAME TABLE", sql)
            or re.search("ALTER TABLE .* RENAME TO", sql),
            message="RENAMING tables",
            mode=CheckMode.ONE_LINER,
            type=CheckType.ERROR,
        ),
        Check(
            code="NOT_NULL",
            fn=has_not_null_column,
            message="NOT NULL constraint on columns",
            mode=CheckMode.TRANSACTION,
            type=CheckType.ERROR,
        ),
        Check(
            code="DROP_COLUMN",
            fn=lambda sql, **kw: re.search("DROP COLUMN", sql),
            message="DROPPING columns",
            mode=CheckMode.ONE_LINER,
            type=CheckType.ERROR,
        ),
        Check(
            code="DROP_TABLE",
            fn=lambda sql, **kw: sql.startswith("DROP TABLE"),
            message="DROPPING table",
            mode=CheckMode.ONE_LINER,
            type=CheckType.ERROR,
        ),
        Check(
            code="RENAME_COLUMN",
            fn=lambda sql, **kw: re.search("ALTER TABLE .* CHANGE", sql)
            or re.search("ALTER TABLE .* RENAME COLUMN", sql),
            message="RENAMING columns",
            mode=CheckMode.ONE_LINER,
            type=CheckType.ERROR,
        ),
        Check(
            code="ALTER_COLUMN",
            fn=lambda sql, **kw: re.search("ALTER TABLE .* ALTER COLUMN .* TYPE", sql),
            message=(
                "ALTERING columns (Could be backward compatible. "
                "You may ignore this migration.)"
            ),
            mode=CheckMode.ONE_LINER,
            type=CheckType.ERROR,
        ),
        Check(
            code="ADD_UNIQUE",
            fn=has_add_unique,
            message="ADDING unique constraint",
            mode=CheckMode.TRANSACTION,
            type=CheckType.ERROR,
        ),
    ]

    migration_checks: list[Check] = []

    def __init__(self, exclude_migration_tests: Iterable[str] | None):
        self.exclude_migration_tests: Iterable[str] = exclude_migration_tests or []
        self.errors: list[Issue] = []
        self.warnings: list[Issue] = []
        self.ignored: list[Issue] = []
        self.migration_checks = update_migration_checks(
            self.base_migration_checks, self.migration_checks
        )

    def analyse(self, sql_statements: list[str]) -> None:
        for statement in sql_statements:
            for test in self.one_line_migration_checks:
                self._check_sql(test, sql=statement)

        for test in self.transaction_migration_checks:
            self._check_sql(test, sql=sql_statements)

    @property
    def one_line_migration_checks(self) -> Iterable[Check]:
        return (c for c in self.migration_checks if c.mode == CheckMode.ONE_LINER)

    @property
    def transaction_migration_checks(self) -> Iterable[Check]:
        return (c for c in self.migration_checks if c.mode == CheckMode.TRANSACTION)

    def _check_sql(self, check: Check, sql: list[str] | str) -> None:
        if check.fn(sql, errors=self.errors):
            if check.code in self.exclude_migration_tests:
                action = "IGNORED"
                list_to_add = self.ignored
            elif check.type == CheckType.WARNING:
                action = "WARNING"
                list_to_add = self.warnings
            else:
                action = "ERROR"
                list_to_add = self.errors
            logger.debug("Testing %s -- %s", sql, action)
            issue = self.build_issue(migration_check=check, sql_statement=sql)
            list_to_add.append(issue)
        else:
            logger.debug("Testing %s -- PASSED", sql)

    def build_issue(
        self, migration_check: Check, sql_statement: list[str] | str
    ) -> Issue:
        table = self.detect_table(sql_statement)
        col = self.detect_column(sql_statement)
        return Issue(
            code=migration_check.code,
            message=migration_check.message,
            table=table,
            column=col,
        )

    @staticmethod
    def detect_table(sql: list[str] | str) -> str | None:
        if isinstance(sql, str):
            regex_result = re.search("TABLE [`\"'](.*?)[`\"']", sql, re.IGNORECASE)
            if regex_result:
                return regex_result.group(1)
        return None

    @staticmethod
    def detect_column(sql: list[str] | str) -> str | None:
        if isinstance(sql, str):
            regex_result = re.search("COLUMN [`\"'](.*?)[`\"']", sql, re.IGNORECASE)
            if regex_result:
                return regex_result.group(1)
        return None
