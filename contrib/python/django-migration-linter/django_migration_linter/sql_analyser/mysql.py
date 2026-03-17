from __future__ import annotations

import re

from .base import BaseAnalyser, Check, CheckMode, CheckType


class MySqlAnalyser(BaseAnalyser):
    migration_checks: list[Check] = [
        Check(
            code="ALTER_COLUMN",
            fn=lambda sql, **kw: re.search("ALTER TABLE .* MODIFY .* (?!NULL);?$", sql),
            message=(
                "ALTERING columns (Could be backward compatible. "
                "You may ignore this migration.)"
            ),
            mode=CheckMode.ONE_LINER,
            type=CheckType.ERROR,
        ),
    ]

    @staticmethod
    def detect_column(sql: list[str] | str) -> str | None:
        if isinstance(sql, str):
            regex_result = re.search("COLUMN [`\"'](.*?)[`\"']", sql, re.IGNORECASE)
            if regex_result:
                return regex_result.group(1)
            regex_result = re.search("MODIFY [`\"'](.*?)[`\"']", sql, re.IGNORECASE)
            if regex_result:
                return regex_result.group(1)
        return None
