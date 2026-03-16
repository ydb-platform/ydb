from __future__ import annotations

import re

from .base import BaseAnalyser, Check, CheckMode, CheckType


class SqliteAnalyser(BaseAnalyser):
    migration_checks: list[Check] = [
        Check(
            code="RENAME_TABLE",
            fn=lambda sql, **kw: re.search("ALTER TABLE .* RENAME TO", sql)
            and "__old" not in sql
            and "new__" not in sql,
            message="RENAMING tables",
            mode=CheckMode.ONE_LINER,
            type=CheckType.ERROR,
        ),
        Check(
            code="DROP_TABLE",
            # TODO: improve to detect that the table names overlap
            fn=lambda sql_statements, **kw: any(
                sql.startswith("DROP TABLE") for sql in sql_statements
            )
            and not any(sql.startswith("CREATE TABLE") for sql in sql_statements),
            message="DROPPING table",
            mode=CheckMode.TRANSACTION,
            type=CheckType.ERROR,
        ),
        Check(
            code="NOT_NULL",
            fn=lambda sql_statements, **kw: any(
                re.search("NOT NULL(?! PRIMARY)(?! DEFAULT)", sql)
                for sql in sql_statements
            )
            and any(
                re.search("ALTER TABLE .* RENAME TO", sql)
                and ("__old" in sql or "new__" in sql)
                for sql in sql_statements
            ),
            message="NOT NULL constraint on columns",
            mode=CheckMode.TRANSACTION,
            type=CheckType.ERROR,
        ),
    ]

    @staticmethod
    def detect_table(sql: list[str] | str) -> str | None:
        if isinstance(sql, str):
            regex_result = re.search("TABLE [`\"'](.*?)[`\"']", sql, re.IGNORECASE)
            if regex_result:
                return regex_result.group(1)
            regex_result = re.search("ON [`\"'](.*?)[`\"']", sql, re.IGNORECASE)
            if regex_result:
                return regex_result.group(1)
        return None
