from __future__ import annotations

from typing import Any

from ._version import __version__
from .prettytable import (  # noqa: F401
    _DEPRECATED_ALL,
    _DEPRECATED_DEFAULT,
    _DEPRECATED_DOUBLE_BORDER,
    _DEPRECATED_FRAME,
    _DEPRECATED_HEADER,
    _DEPRECATED_MARKDOWN,
    _DEPRECATED_MSWORD_FRIENDLY,
    _DEPRECATED_NONE,
    _DEPRECATED_ORGMODE,
    _DEPRECATED_PLAIN_COLUMNS,
    _DEPRECATED_RANDOM,
    _DEPRECATED_SINGLE_BORDER,
    HRuleStyle,
    PrettyTable,
    RowType,
    TableHandler,
    TableStyle,
    VRuleStyle,
    _warn_deprecation,
    from_csv,
    from_db_cursor,
    from_html,
    from_html_one,
    from_json,
    from_mediawiki,
)

__all__ = [
    "ALL",
    "DEFAULT",
    "DOUBLE_BORDER",
    "FRAME",
    "HEADER",
    "MARKDOWN",
    "MSWORD_FRIENDLY",
    "NONE",
    "ORGMODE",
    "PLAIN_COLUMNS",
    "RANDOM",
    "SINGLE_BORDER",
    "HRuleStyle",
    "PrettyTable",
    "RowType",
    "TableHandler",
    "TableStyle",
    "VRuleStyle",
    "__version__",
    "from_csv",
    "from_db_cursor",
    "from_html",
    "from_html_one",
    "from_json",
    "from_mediawiki",
]


def __getattr__(name: str) -> Any:
    return _warn_deprecation(name, module_globals=globals())
