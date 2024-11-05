from __future__ import annotations

from typing import Any

from .prettytable import (
    ALL,
    DEFAULT,
    DOUBLE_BORDER,
    FRAME,
    HEADER,
    MARKDOWN,
    MSWORD_FRIENDLY,
    NONE,
    ORGMODE,
    PLAIN_COLUMNS,
    RANDOM,
    SINGLE_BORDER,
    PrettyTable,
    TableHandler,
    from_csv,
    from_db_cursor,
    from_html,
    from_html_one,
    from_json,
)

__all__ = [
    "ALL",
    "DEFAULT",
    "DOUBLE_BORDER",
    "SINGLE_BORDER",
    "FRAME",
    "HEADER",
    "MARKDOWN",
    "MSWORD_FRIENDLY",
    "NONE",
    "ORGMODE",
    "PLAIN_COLUMNS",
    "RANDOM",
    "PrettyTable",
    "TableHandler",
    "from_csv",
    "from_db_cursor",
    "from_html",
    "from_html_one",
    "from_json",
]


def __getattr__(name: str) -> Any:
    if name == "__version__":
        import importlib.metadata

        return importlib.metadata.version(__name__)

    msg = f"module '{__name__}' has no attribute '{name}'"
    raise AttributeError(msg)
