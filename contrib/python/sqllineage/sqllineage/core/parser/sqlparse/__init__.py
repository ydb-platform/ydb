import re

from sqlparse import tokens
from sqlparse.keywords import KEYWORDS, SQL_REGEX


def _patch_adding_builtin_type() -> None:
    KEYWORDS["STRING"] = tokens.Name.Builtin
    KEYWORDS["DATETIME"] = tokens.Name.Builtin


def _patch_updating_lateral_view_lexeme() -> None:
    for i, (regex, lexeme) in enumerate(SQL_REGEX):
        rgx = re.compile(regex, re.IGNORECASE | re.UNICODE).match
        if rgx("LATERAL VIEW EXPLODE(col)"):
            new_regex = r"(LATERAL\s+VIEW\s+)(OUTER\s+)?(EXPLODE|INLINE|PARSE_URL_TUPLE|POSEXPLODE|STACK|JSON_TUPLE)\b"
            SQL_REGEX[i] = (new_regex, lexeme)
            break


def _monkey_patch() -> None:
    _patch_adding_builtin_type()
    _patch_updating_lateral_view_lexeme()


_monkey_patch()
