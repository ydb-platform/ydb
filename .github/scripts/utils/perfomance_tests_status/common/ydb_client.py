"""Thin YDB client for perfomance_tests_status — wraps analytics YDBWrapper.

Auth: path in ``CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`` (or
``YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS``). Locally load via::

    eval "$(python3 duty_agent/dutyctl.py init-token --shell)"
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parent.parent  # perfomance_tests_status
REPO_ROOT = ROOT.parents[3]  # …/utils → scripts → .github → repo
ANALYTICS = REPO_ROOT / ".github" / "scripts" / "analytics"
DEFAULT_CONFIG = REPO_ROOT / ".github" / "config" / "ydb_qa_config.json"

SA_ENV = "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
SA_ENV_ALT = "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"


class YdbClientError(RuntimeError):
    """Missing credentials or YDB call failure."""


def analytics_path() -> Path:
    return ANALYTICS


def ensure_analytics_on_path() -> None:
    """Put analytics on sys.path without putting repo root (shadows pip ``ydb``)."""
    ap = str(ANALYTICS)
    if ap not in sys.path:
        sys.path.insert(0, ap)


def resolve_sa_key_path(sa_key_file: str | Path | None = None) -> Path:
    """Resolve SA JSON path from arg or env; raise if missing/invalid."""
    raw = sa_key_file
    if raw is None:
        raw = os.environ.get(SA_ENV) or os.environ.get(SA_ENV_ALT)
    if not raw:
        raise YdbClientError(
            f"Need --sa-key-file or env {SA_ENV}. "
            'Run: eval "$(python3 dutyctl.py init-token --shell)" '
            "(from duty_agent/) to load YAV SA key."
        )
    path = Path(str(raw)).expanduser().resolve()
    if not path.is_file():
        raise YdbClientError(f"SA key not found: {path}")
    return path


def ensure_sa_credentials(sa_key_file: str | Path | None = None) -> Path:
    """Set CI/YDB SA env vars to an existing key file; return path."""
    path = resolve_sa_key_path(sa_key_file)
    os.environ[SA_ENV] = str(path)
    os.environ[SA_ENV_ALT] = str(path)
    return path


def jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if hasattr(v, "as_py"):
        return jsonable(v.as_py())
    return str(v)


def row_to_obj(row: Any) -> dict[str, Any]:
    return {k: jsonable(v) for k, v in dict(row).items()}


def to_result_sets(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Shape consumed by generate.load_rows / dig_runs.rows_from_result_json."""
    cols = list(rows[0].keys()) if rows else []
    return {
        "result_sets": [
            {
                "columns": cols,
                "rows": [[r.get(c) for c in cols] for r in rows],
            }
        ]
    }


# Backward-compatible alias (old MCP-shaped dumps).
to_mcp_shaped = to_result_sets


@contextmanager
def open_wrapper(
    *,
    script_name: str,
    config: str | Path | None = None,
    use_local_config: bool | None = True,
    enable_statistics: bool = False,
    silent: bool = False,
) -> Iterator[Any]:
    """Context manager yielding ``YDBWrapper`` (credentials must already be set)."""
    ensure_analytics_on_path()
    from ydb_wrapper import YDBWrapper  # noqa: E402

    cfg = str(config or DEFAULT_CONFIG)
    with YDBWrapper(
        config_path=cfg,
        enable_statistics=enable_statistics,
        script_name=script_name,
        silent=silent,
        use_local_config=use_local_config,
    ) as ydb_w:
        if not ydb_w.check_credentials():
            raise YdbClientError("YDB credentials check failed")
        yield ydb_w


def scan_query(
    sql: str,
    *,
    query_name: str = "scan",
    script_name: str = "perfomance_tests_status/ydb_client",
    sa_key_file: str | Path | None = None,
    config: str | Path | None = None,
    use_local_config: bool | None = True,
) -> list[dict[str, Any]]:
    """Run streaming scan query; return list of jsonable dicts."""
    ensure_sa_credentials(sa_key_file)
    with open_wrapper(
        script_name=script_name,
        config=config,
        use_local_config=use_local_config,
    ) as ydb_w:
        rows = ydb_w.execute_scan_query(sql, query_name=query_name)
    return [row_to_obj(r) for r in rows]


def ping(
    *,
    sa_key_file: str | Path | None = None,
    config: str | Path | None = None,
    use_local_config: bool | None = True,
    script_name: str = "perfomance_tests_status/ydb_client",
) -> bool:
    """Smoke check: ``SELECT 1 AS ok``. Raises on failure."""
    rows = scan_query(
        "SELECT 1 AS ok;",
        query_name="ping",
        script_name=script_name,
        sa_key_file=sa_key_file,
        config=config,
        use_local_config=use_local_config,
    )
    if not rows:
        raise YdbClientError("ping: empty result")
    return True
