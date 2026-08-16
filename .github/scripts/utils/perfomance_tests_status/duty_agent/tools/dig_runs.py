"""Build / summarize dig queries against perfomance/tpcc and olap marts (Now report sources)."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"

# Default lookback: pack suite_history is short; mart dig must be wider for correlations.
DEFAULT_DAYS_BEFORE = 35
DEFAULT_DAYS_AFTER = 3

# Nearest Allure URL from tests_results onto mart dig rows.
TPCC_REPORT_MATCH_MAX_SEC = 6 * 3600

# Map UI db alias → cluster column in perfomance/tpcc (best-effort).
TPCC_DB_TO_CLUSTER = {
    "perf3": "perf3",
    "perf4": "perf4",
    "perf9": "perf9",
}

# Suite family → related Suite prefixes for OLAP neighbor dig (correlation).
OLAP_RELATED_PREFIXES: dict[str, tuple[str, ...]] = {
    "UploadTpch": ("UploadTpch", "Tpch"),
    "Tpch": ("Tpch", "UploadTpch"),
    "Tpcds": ("Tpcds",),
    "Clickbench": ("Clickbench",),
    "WorkloadManager": ("WorkloadManager",),
    "Upload": ("Upload",),
}


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _escape_ydb_str(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


# Pack/UI often says ``trunk`` (Arc CI); mart OLAP rows usually store ``main`` /
# ``origin/main`` (and sometimes empty Branch with Version ``.sha`` / ``trunk.r…``).
_TRUNK_MAIN = frozenset({"main", "trunk"})


def _branch_cores(branch: str) -> set[str]:
    """Canonical branch names to match in SQL / post-filter (main ↔ trunk)."""
    raw = (branch or "").strip().lower()
    if not raw:
        return set(_TRUNK_MAIN)
    core = raw.removeprefix("origin/")
    if "/" in core:
        core = core.rsplit("/", 1)[-1]
    if core in _TRUNK_MAIN:
        return set(_TRUNK_MAIN)
    return {core} if core else set(_TRUNK_MAIN)


def _branch_clause(column: str, branch: str) -> str:
    cores = sorted(_branch_cores(branch))
    parts: list[str] = []
    for c in cores:
        ec = _escape_ydb_str(c)
        parts.append(
            f"({column} = '{ec}' OR {column} = 'origin/{ec}' "
            f"OR EndsWith(CAST({column} AS String), '/{ec}'))"
        )
    # Acceptance OLAP often leaves Branch empty while CiVersion is trunk.r…
    if set(cores) & _TRUNK_MAIN:
        parts.append(f"(CAST({column} AS String) = '' OR {column} IS NULL)")
    if len(parts) == 1:
        return parts[0]
    return "(" + " OR ".join(parts) + ")"


def _olap_family(suite: str) -> str:
    for prefix in (
        "UploadTpch",
        "Tpch",
        "Tpcds",
        "Clickbench",
        "WorkloadManager",
        "Upload",
    ):
        if suite.startswith(prefix):
            return prefix
    return suite


def selection_from_ctx(ctx: dict[str, Any]) -> dict[str, Any]:
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    suite = str(sel.get("suite") or "")
    run_type = sel.get("run_type")
    warehouses = sel.get("warehouses")
    if "@" in suite:
        left, _, right = suite.partition("@")
        if warehouses is None and right.isdigit():
            warehouses = int(right)
        suite_rt = f"ydb_cli_{left}" if not str(left).startswith("ydb_cli_") else left
        # Pack often stores short run_type ("default"); mart uses ydb_cli_*
        if (
            run_type is None
            or not str(run_type).startswith("ydb_cli_")
            or str(run_type) in ("default", "latency")
        ):
            run_type = suite_rt
    db = str(sel.get("db") or "")
    cluster = TPCC_DB_TO_CLUSTER.get(db, db)
    branch = str(sel.get("branch") or "main")
    focus_ts = _parse_ts(fr.get("ts") or fr.get("day"))
    family = sel.get("family") or (_olap_family(suite) if suite else None)
    return {
        "kind": str((ctx.get("report") or {}).get("kind") or ""),
        "cluster": cluster,
        "db": db,
        "branch": branch,
        "suite": suite,
        "run_type": run_type,
        "warehouses": warehouses,
        "focus_sha": fr.get("sha"),
        "focus_label": fr.get("label"),
        "focus_ts": _iso(focus_ts) if focus_ts else None,
        "family": family,
    }


def window_from_focus(
    focus_ts: str | None,
    *,
    days_before: int = DEFAULT_DAYS_BEFORE,
    days_after: int = DEFAULT_DAYS_AFTER,
) -> tuple[str, str]:
    dt = _parse_ts(focus_ts) or datetime.now(timezone.utc)
    since = dt - timedelta(days=int(days_before))
    until = dt + timedelta(days=int(days_after))
    return _iso(since), _iso(until)


def _mart_cluster_to_ci(cluster: str) -> str:
    c = (cluster or "").strip().lower()
    m = re.fullmatch(r"perf(\d+)", c)
    if m:
        return f"oltp-perf-{m.group(1)}"
    return c


def _allure_suite_for(run_type: str, warehouses: Any) -> str | None:
    fam = (run_type or "").lower()
    if fam.startswith("ydb_cli_"):
        fam = fam[len("ydb_cli_") :]
    if "snapshot" in fam:
        mode = "Snapshot"
    elif "serializable" in fam:
        mode = "Serializable"
    else:
        return None
    try:
        wh = int(warehouses)
    except (TypeError, ValueError):
        return None
    if wh <= 0:
        return None
    return f"TpccW{wh}T0{mode}"


def build_tpcc_reports_sql(*, since: str, until: str) -> str:
    """Allure URLs for TPC-C suites in the dig window (tests_results)."""
    return f"""SELECT
  Suite,
  Test,
  JSON_VALUE(Info, '$.ci_cluster_name') AS ci_cluster_name,
  JSON_VALUE(Info, '$.report_url') AS report_url,
  Timestamp AS timestamp
FROM `perfomance/olap/tests_results`
WHERE Timestamp >= Timestamp('{_escape_ydb_str(since)}')
  AND Timestamp <= Timestamp('{_escape_ydb_str(until)}')
  AND StartsWith(Suite, 'TpccW')
  AND Test = 'test'
  AND JSON_VALUE(Info, '$.report_url') IS NOT NULL
ORDER BY Timestamp;
"""


def enrich_tpcc_rows_with_reports(
    rows: list[dict[str, Any]],
    report_rows: list[dict[str, Any]],
    *,
    max_delta_sec: int = TPCC_REPORT_MATCH_MAX_SEC,
) -> list[dict[str, Any]]:
    """Attach Report (= Allure URL) onto mart dig rows by cluster/suite/nearest ts."""
    by_key: dict[tuple[str, str], list[tuple[datetime, str]]] = defaultdict(list)
    for d in report_rows:
        url = str(d.get("report_url") or d.get("Report") or "").strip()
        suite = str(d.get("Suite") or "")
        ci = str(d.get("ci_cluster_name") or "").lower()
        ts = _parse_ts(str(d.get("timestamp") or ""))
        if not url or not suite or not ci or ts is None:
            continue
        by_key[(ci, suite)].append((ts, url))
    for lst in by_key.values():
        lst.sort(key=lambda x: x[0])

    out: list[dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        suite = _allure_suite_for(str(r.get("run_type") or ""), r.get("warehouses"))
        ci = _mart_cluster_to_ci(str(r.get("cluster") or ""))
        ts = _parse_ts(str(r.get("timestamp") or ""))
        best = None
        best_delta = None
        if suite and ts is not None:
            for rts, url in by_key.get((ci, suite), []):
                delta = abs((rts - ts).total_seconds())
                if delta > max_delta_sec:
                    continue
                if best_delta is None or delta < best_delta:
                    best_delta = delta
                    best = url
        if best:
            row["Report"] = best
        out.append(row)
    return out


def build_tpcc_sql(
    *,
    since: str,
    until: str,
    cluster: str | None = None,
    run_type: str | None = None,
    warehouses: int | None = None,
    branch: str | None = None,
    neighbors: bool = True,
) -> str:
    """SQL for mart dig.

    neighbors=True (default): all ydb_cli_* run_types on **all clusters**, same branch.
    neighbors=False / slice_only: only the alert run_type@warehouses@cluster.
    """
    clauses = [
        f"timestamp >= Timestamp('{_escape_ydb_str(since)}')",
        f"timestamp <= Timestamp('{_escape_ydb_str(until)}')",
        "run_type LIKE 'ydb_cli_%'",
    ]
    if branch:
        clauses.append(_branch_clause("git_branch", branch))
    if not neighbors:
        if run_type:
            clauses.append(f"run_type = '{_escape_ydb_str(str(run_type))}'")
        if warehouses is not None:
            clauses.append(f"warehouses = {int(warehouses)}")
        if cluster:
            clauses.append(f"cluster = '{_escape_ydb_str(cluster)}'")

    where = " AND ".join(clauses)
    return f"""SELECT
  cluster,
  run_type,
  warehouses,
  COALESCE(CAST(git_branch AS String), '') AS git_branch,
  timestamp,
  git_commit_timestamp,
  tpmC,
  newOrderLatency90 AS lat90,
  efficiency,
  version
FROM `perfomance/tpcc`
WHERE {where}
ORDER BY cluster, run_type, warehouses, timestamp;
"""


def build_olap_sql(
    *,
    since: str,
    until: str,
    db_alias: str | None = None,
    suite: str | None = None,
    branch: str | None = None,
    neighbors: bool = True,
) -> str:
    """SQL for OLAP mart dig.

    neighbors=True: same branch; related suite families + all DbAlias (peer clusters).
    slice_only: focus Suite + DbAlias (+ branch).
    """
    clauses = [
        f"RunTs >= Timestamp('{_escape_ydb_str(since)}')",
        f"RunTs <= Timestamp('{_escape_ydb_str(until)}')",
    ]
    if branch:
        clauses.append(_branch_clause("Branch", branch))
    if not neighbors:
        if suite:
            clauses.append(f"Suite = '{_escape_ydb_str(suite)}'")
        if db_alias:
            clauses.append(f"DbAlias = '{_escape_ydb_str(db_alias)}'")
    elif suite:
        fam = _olap_family(suite)
        prefixes = OLAP_RELATED_PREFIXES.get(fam, (fam,))
        or_parts = [
            f"StartsWith(Suite, '{_escape_ydb_str(p)}')" for p in prefixes
        ]
        # Always include exact focus suite
        or_parts.append(f"Suite = '{_escape_ydb_str(suite)}'")
        clauses.append("(" + " OR ".join(or_parts) + ")")
        # all DbAlias on branch — do not filter db_alias

    where = " AND ".join(clauses)
    return f"""SELECT
  Branch,
  Version,
  DbAlias,
  Suite,
  RunTs,
  YdbSumMeans,
  GrossTime,
  SuccessCount,
  FailCount,
  FailTests,
  Report
FROM `perfomance/olap/fast_results_siutes`
WHERE {where}
ORDER BY RunTs, DbAlias, Suite;
"""


def build_dig_sql(
    ctx: dict[str, Any],
    *,
    neighbors: bool = True,
    days_before: int = DEFAULT_DAYS_BEFORE,
    days_after: int = DEFAULT_DAYS_AFTER,
) -> dict[str, Any]:
    sel = selection_from_ctx(ctx)
    since, until = window_from_focus(
        sel.get("focus_ts"),
        days_before=days_before,
        days_after=days_after,
    )
    kind = sel.get("kind") or "olap"
    reports_sql = None
    if kind == "tpcc":
        sql = build_tpcc_sql(
            since=since,
            until=until,
            cluster=sel.get("cluster"),
            run_type=sel.get("run_type"),
            warehouses=sel.get("warehouses"),
            branch=sel.get("branch"),
            neighbors=neighbors,
        )
        table = "perfomance/tpcc"
        reports_sql = build_tpcc_reports_sql(since=since, until=until)
    else:
        sql = build_olap_sql(
            since=since,
            until=until,
            db_alias=sel.get("db"),
            suite=sel.get("suite"),
            branch=sel.get("branch"),
            neighbors=neighbors,
        )
        table = "perfomance/olap/fast_results_siutes"
    return {
        "kind": kind,
        "table": table,
        "since": since,
        "until": until,
        "days_before": days_before,
        "days_after": days_after,
        "selection": sel,
        "neighbors": neighbors,
        "sql": sql,
        "reports_sql": reports_sql,
        "fetch_hint": (
            "Default: dutyctl dig-runs -c CONTEXT -o OUT  (executes via common/ydb_client; "
            "needs CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS from init-token). "
            "TPC-C also fetches Allure URLs from tests_results and attaches Report on slice_runs. "
            "Offline: --from-json dig_runs_raw.json. SQL only: --sql-only. "
            "If jump sits at the edge of the window, re-run with --days-before 60|90."
        ),
        # legacy key for older callers
        "mcp_hint": (
            "Deprecated: use dutyctl dig-runs (ydb_client). "
            "Offline: --from-json dig_runs_raw.json."
        ),
    }


def rows_from_result_json(payload: Any) -> list[dict[str, Any]]:
    """Accept {result_sets:[{columns,rows}]} or list[dict] or {rows:…}."""
    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict) and "columns" not in payload[0]:
            return [r for r in payload if isinstance(r, dict)]
        if payload and isinstance(payload[0], dict) and "columns" in payload[0]:
            return _result_set_to_rows(payload[0])
        return []
    if not isinstance(payload, dict):
        return []
    if "result_sets" in payload:
        sets = payload.get("result_sets") or []
        if sets:
            return _result_set_to_rows(sets[0])
    if "columns" in payload and "rows" in payload:
        return _result_set_to_rows(payload)
    if isinstance(payload.get("rows"), list):
        rows = payload["rows"]
        if rows and isinstance(rows[0], dict):
            return rows
    return []


# Backward-compatible alias (old fixture / test name)
rows_from_mcp_json = rows_from_result_json


def _result_set_to_rows(rs: dict[str, Any]) -> list[dict[str, Any]]:
    cols = rs.get("columns") or []
    names: list[str] = []
    for c in cols:
        if isinstance(c, dict):
            names.append(str(c.get("name") or c.get("id") or f"c{len(names)}"))
        else:
            names.append(str(c))
    out: list[dict[str, Any]] = []
    for row in rs.get("rows") or []:
        if isinstance(row, dict):
            out.append(row)
            continue
        if isinstance(row, (list, tuple)):
            out.append({names[i]: row[i] if i < len(row) else None for i in range(len(names))})
    return out


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _branch_ok(b: str, branch: str) -> bool:
    b = (b or "").lower()
    bl = branch.lower()
    return bl in b or b.endswith("/" + bl) or b == bl


def _largest_metric_step(
    series: list[dict[str, Any]],
    metric: str,
) -> dict[str, Any] | None:
    step = None
    for a, b in zip(series, series[1:]):
        va, vb = _num(a.get(metric)), _num(b.get(metric))
        if va is None or vb is None:
            continue
        delta = vb - va
        if step is None or abs(delta) > abs(step["delta"]):
            step = {
                "metric": metric,
                "from_ts": a.get("timestamp") or a.get("RunTs"),
                "to_ts": b.get("timestamp") or b.get("RunTs"),
                "from_version": a.get("version") or a.get("Version"),
                "to_version": b.get("version") or b.get("Version"),
                "from": va,
                "to": vb,
                "delta": delta,
            }
    return step


# Suite-level "ok": FailCount==0 and Ydb not an outlier vs median of greens.
_STABLE_MIN_STREAK = 3
_YDB_LO = 0.55  # below → likely incomplete / collapsed suite duration
_YDB_HI = 2.0   # above → duration spike, not a calm baseline


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    xs = sorted(vals)
    mid = len(xs) // 2
    if len(xs) % 2:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0


def _olap_suite_ok(
    r: dict[str, Any],
    *,
    ydb_median: float | None,
) -> bool:
    """Whole-suite health — not a single query. FailCount + duration band."""
    fc = _num(r.get("FailCount"))
    if fc is None or fc > 0:
        return False
    ydb = _num(r.get("YdbSumMeans"))
    if ydb is None or ydb_median is None or ydb_median <= 0:
        return True
    return (_YDB_LO * ydb_median) <= ydb <= (_YDB_HI * ydb_median)


def _olap_ydb_median_greens(slice_before_focus: list[dict[str, Any]]) -> float | None:
    ys = [
        y
        for r in slice_before_focus
        if (_num(r.get("FailCount")) is not None and _num(r.get("FailCount")) <= 0)
        for y in (_num(r.get("YdbSumMeans")),)
        if y is not None and y > 0
    ]
    return _median(ys)


def find_olap_stable_plateau(
    slice_runs: list[dict[str, Any]],
    *,
    min_streak: int = _STABLE_MIN_STREAK,
) -> dict[str, Any] | None:
    """
    End of the latest *suite-stable* streak before focus.

    Stable ≠ nearest FailCount=0. Need a streak of suite-ok runs
    (FailCount==0 and YdbSumMeans in band around median greens).
    Returns the last run of that streak (+ meta).
    """
    if len(slice_runs) < 2:
        return None
    before = list(slice_runs[:-1])
    ydb_med = _olap_ydb_median_greens(before)
    ok_flags = [_olap_suite_ok(r, ydb_median=ydb_med) for r in before]

    # Collect streaks (start_i, end_i inclusive) of consecutive ok.
    streaks: list[tuple[int, int]] = []
    i = 0
    n = len(before)
    while i < n:
        if not ok_flags[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and ok_flags[j + 1]:
            j += 1
        streaks.append((i, j))
        i = j + 1
    if not streaks:
        return None

    # Prefer latest streak with length >= min_streak; else latest with >=2; else latest any.
    chosen: tuple[int, int] | None = None
    for need in (min_streak, 2, 1):
        for s, e in reversed(streaks):
            if (e - s + 1) >= need:
                chosen = (s, e)
                break
        if chosen:
            break
    if not chosen:
        return None
    s, e = chosen
    end = before[e]
    streak_len = e - s + 1
    return {
        "RunTs": end.get("RunTs"),
        "Version": end.get("Version"),
        "FailCount": end.get("FailCount"),
        "YdbSumMeans": _num(end.get("YdbSumMeans")),
        "Report": end.get("Report"),
        "streak_len": streak_len,
        "streak_start_Version": before[s].get("Version"),
        "ydb_median_greens": ydb_med,
        "min_streak_required": min_streak,
        "weak": streak_len < min_streak,
    }


def _olap_last_stable_before_focus(
    slice_runs: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Compat alias: plateau end (not nearest FailCount=0)."""
    plate = find_olap_stable_plateau(slice_runs)
    if not plate:
        return None
    return {
        "RunTs": plate.get("RunTs"),
        "Version": plate.get("Version"),
        "FailCount": plate.get("FailCount"),
        "YdbSumMeans": plate.get("YdbSumMeans"),
        "Report": plate.get("Report"),
        "streak_len": plate.get("streak_len"),
        "weak": plate.get("weak"),
    }


def build_olap_pr_window(
    slice_runs: list[dict[str, Any]],
    *,
    fail_jump: dict[str, Any] | None,
    ydb_jump: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    Window for dig-prs / code bisect.

    Prefer end of latest suite-stable streak (FailCount=0 + normal YdbSumMeans,
    ≥3 runs) → focus. Not the nearest single FailCount=0 (fluke green).
    Duration-only regressions → largest_ydb_step.
    """
    if not slice_runs:
        return None
    focus = slice_runs[-1]
    head = focus.get("Version")
    if not head:
        return None
    focus_fc = _num(focus.get("FailCount"))
    focus_ydb = _num(focus.get("YdbSumMeans"))
    plate = find_olap_stable_plateau(slice_runs)

    # Fail / flaky suite: dig from end of last calm plateau.
    if plate and plate.get("Version") and str(plate.get("Version")) != str(head):
        if focus_fc is None or focus_fc > 0 or plate.get("streak_len", 0) >= 2:
            src = (
                "stable_streak_end"
                if not plate.get("weak")
                else "stable_streak_end_weak"
            )
            return {
                "base": plate.get("Version"),
                "head": head,
                "source": src,
                "reason": (
                    f"конец серии suite-ok (FailCount=0 + Ydb≈median, "
                    f"streak={plate.get('streak_len')}, "
                    f"с {plate.get('streak_start_Version')}) → разбираемый; "
                    "не одиночный FailCount=0"
                ),
                "base_FailCount": 0,
                "head_FailCount": focus_fc,
                "base_Report": plate.get("Report"),
                "streak_len": plate.get("streak_len"),
                "streak_start_Version": plate.get("streak_start_Version"),
                "ydb_median_greens": plate.get("ydb_median_greens"),
            }

    # Pure duration regression (suite still FailCount=0 but Ydb jumped).
    if (
        (focus_fc is not None and focus_fc <= 0)
        and ydb_jump
        and ydb_jump.get("from_version")
        and focus_ydb is not None
    ):
        return {
            "base": ydb_jump.get("from_version"),
            "head": ydb_jump.get("to_version") or head,
            "source": "largest_ydb_step",
            "reason": "наибольший скачок YdbSumMeans в mart (suite без FailCount)",
            "delta": ydb_jump.get("delta"),
        }
    if ydb_jump and ydb_jump.get("from_version") and not plate:
        return {
            "base": ydb_jump.get("from_version"),
            "head": ydb_jump.get("to_version") or head,
            "source": "largest_ydb_step",
            "reason": "наибольший скачок YdbSumMeans в mart",
            "delta": ydb_jump.get("delta"),
        }
    if fail_jump and fail_jump.get("from_version"):
        return {
            "base": fail_jump.get("from_version"),
            "head": head,
            "source": "largest_fail_step_to_focus",
            "reason": (
                "скачок FailCount в окне dig → голова = разбираемый Version "
                "(не промежуточный to_version)"
            ),
            "delta": fail_jump.get("delta"),
        }
    return None


def summarize_tpcc_rows(
    rows: list[dict[str, Any]],
    *,
    selection: dict[str, Any],
) -> dict[str, Any]:
    cluster = selection.get("cluster")
    branch = selection.get("branch") or "main"
    focus_sha = str(selection.get("focus_sha") or "")[:7]
    want_rt = str(selection.get("run_type") or "")
    want_wh = selection.get("warehouses")

    def ts_key(r: dict[str, Any]) -> str:
        return str(r.get("timestamp") or "")

    def match_focus_suite(r: dict[str, Any]) -> bool:
        if str(r.get("run_type") or "") != want_rt:
            return False
        if want_wh is not None and int(r.get("warehouses") or -1) != int(want_wh):
            return False
        return True

    focus_slice = [
        r
        for r in rows
        if str(r.get("cluster") or "") == str(cluster)
        and _branch_ok(str(r.get("git_branch") or ""), branch)
        and match_focus_suite(r)
    ]
    focus_slice.sort(key=ts_key)

    # Jump on focus suite only (do not mix latency@12k with default@20k)
    jump = None
    for a, b in zip(focus_slice, focus_slice[1:]):
        la, lb = _num(a.get("lat90")), _num(b.get("lat90"))
        if la is None or lb is None:
            continue
        delta = lb - la
        if jump is None or abs(delta) > abs(jump["lat_delta"]):
            jump = {
                "from_ts": a.get("timestamp"),
                "to_ts": b.get("timestamp"),
                "from_version": a.get("version"),
                "to_version": b.get("version"),
                "from_lat90": la,
                "to_lat90": lb,
                "lat_delta": delta,
                "from_tpmc": _num(a.get("tpmC")),
                "to_tpmc": _num(b.get("tpmC")),
            }

    focus_row = None
    for r in reversed(focus_slice):
        ver = str(r.get("version") or "")
        if focus_sha and ver.startswith(focus_sha):
            focus_row = r
            break
    if focus_row is None and focus_slice:
        focus_row = focus_slice[-1]

    other_clusters: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        c = str(r.get("cluster") or "")
        if c == str(cluster):
            continue
        if not _branch_ok(str(r.get("git_branch") or ""), branch):
            continue
        other_clusters.setdefault(c, []).append(r)
    for c in other_clusters:
        other_clusters[c].sort(key=ts_key)

    peer_snapshot = []
    for c, lst in sorted(other_clusters.items()):
        if not lst:
            continue
        same_rt = [r for r in lst if match_focus_suite(r)]
        pool = same_rt or lst
        series = [
            {
                "timestamp": r.get("timestamp"),
                "version": str(r.get("version") or "")[:7],
                "lat90": _num(r.get("lat90")),
                "tpmC": _num(r.get("tpmC")),
            }
            for r in pool
        ]
        last = pool[-1]
        peer_jump = _largest_metric_step(
            [
                {
                    "timestamp": r.get("timestamp"),
                    "version": r.get("version"),
                    "lat90": _num(r.get("lat90")),
                }
                for r in pool
            ],
            "lat90",
        )
        peer_snapshot.append(
            {
                "cluster": c,
                "timestamp": last.get("timestamp"),
                "version": last.get("version"),
                "lat90": _num(last.get("lat90")),
                "tpmC": _num(last.get("tpmC")),
                "run_type": last.get("run_type"),
                "warehouses": last.get("warehouses"),
                "n_runs_in_window": len(pool),
                "largest_lat_step": (
                    {
                        "from_version": peer_jump["from_version"],
                        "to_version": peer_jump["to_version"],
                        "from_lat90": peer_jump["from"],
                        "to_lat90": peer_jump["to"],
                        "lat_delta": peer_jump["delta"],
                    }
                    if peer_jump
                    else None
                ),
                "recent": series[-4:],
            }
        )

    # Cross run_type on focus cluster+branch
    by_suite: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        if str(r.get("cluster") or "") != str(cluster):
            continue
        if not _branch_ok(str(r.get("git_branch") or ""), branch):
            continue
        key = f"{r.get('run_type')}@{r.get('warehouses')}"
        by_suite.setdefault(key, []).append(r)
    cross: list[dict[str, Any]] = []
    for key, lst in sorted(by_suite.items()):
        lst = sorted(lst, key=ts_key)
        series = [
            {
                "timestamp": r.get("timestamp"),
                "version": str(r.get("version") or "")[:7],
                "lat90": _num(r.get("lat90")),
                "tpmC": _num(r.get("tpmC")),
            }
            for r in lst
        ]
        step = None
        for a, b in zip(series, series[1:]):
            if a["lat90"] is None or b["lat90"] is None:
                continue
            d = b["lat90"] - a["lat90"]
            if step is None or abs(d) > abs(step["lat_delta"]):
                step = {
                    "from_version": a["version"],
                    "to_version": b["version"],
                    "from_lat90": a["lat90"],
                    "to_lat90": b["lat90"],
                    "lat_delta": d,
                }
        cross.append(
            {
                "suite": key,
                "n": len(series),
                "largest_lat_step": step,
                "runs": series[-8:],
            }
        )

    edge_hint = None
    if jump and focus_slice:
        first_ts = focus_slice[0].get("timestamp")
        if jump.get("from_ts") == first_ts:
            edge_hint = (
                "largest lat step starts at the first point in the dig window — "
                "re-run dig-runs with larger --days-before (60/90)"
            )

    slice_runs = [
        {
            "timestamp": r.get("timestamp"),
            "version": r.get("version"),
            "lat90": _num(r.get("lat90")),
            "tpmC": _num(r.get("tpmC")),
            "efficiency": _num(r.get("efficiency")),
            "cluster": r.get("cluster"),
            "git_branch": r.get("git_branch"),
            "run_type": r.get("run_type"),
            "warehouses": r.get("warehouses"),
            "Report": r.get("Report"),
        }
        for r in focus_slice
    ]
    from .baseline import select_baseline_from_slice_runs

    # Map tpcc jump shape → from_ts for baseline picker
    jump_for_base = None
    if jump:
        jump_for_base = dict(jump)
        if not jump_for_base.get("from_ts"):
            fv = str(jump.get("from_version") or "")[:7]
            for r in slice_runs:
                ver = str(r.get("version") or "")
                if fv and (ver.startswith(fv) or fv in ver):
                    jump_for_base["from_ts"] = r.get("timestamp")
                    break
    baseline_candidate = select_baseline_from_slice_runs(
        [
            {
                **r,
                "RunTs": r.get("timestamp"),
                "Version": r.get("version"),
                "lat90": r.get("lat90"),
            }
            for r in slice_runs
        ],
        metric="lat90",
        jump=jump_for_base,
        focus_version=focus_sha[:7] if focus_sha else None,
    )
    pr_window = None
    if jump and jump.get("from_version"):
        head_ver = None
        if slice_runs:
            head_ver = slice_runs[-1].get("version")
        if not head_ver and focus_sha:
            head_ver = focus_sha
        pr_window = {
            "base": jump.get("from_version"),
            "head": head_ver or jump.get("to_version"),
            "source": "largest_lat_step",
            "reason": "наибольший скачок lat90 в mart; голова = разбираемый Version",
            "lat_delta": jump.get("lat_delta"),
        }
    return {
        "slice_runs": slice_runs,
        "largest_lat_step": jump,
        "pr_window": pr_window,
        "baseline_candidate": baseline_candidate,
        "focus_row": focus_row,
        "peer_clusters_latest": peer_snapshot,
        "cross_run_type": cross,
        "row_count": len(rows),
        "slice_count": len(focus_slice),
        "window_edge_hint": edge_hint,
    }


def summarize_olap_rows(
    rows: list[dict[str, Any]],
    *,
    selection: dict[str, Any],
) -> dict[str, Any]:
    db = selection.get("db")
    suite = selection.get("suite")
    branch = selection.get("branch") or "main"

    def ts_key(r: dict[str, Any]) -> str:
        return str(r.get("RunTs") or "")

    filtered = [
        r for r in rows if _branch_ok(str(r.get("Branch") or ""), branch)
    ]

    focus_slice = [
        r
        for r in filtered
        if str(r.get("DbAlias") or "") == str(db)
        and (not suite or str(r.get("Suite") or "") == suite)
    ]
    focus_slice.sort(key=ts_key)

    fail_jump = _largest_metric_step(
        [
            {
                "RunTs": r.get("RunTs"),
                "Version": r.get("Version"),
                "FailCount": _num(r.get("FailCount")),
            }
            for r in focus_slice
        ],
        "FailCount",
    )
    ydb_jump = _largest_metric_step(
        [
            {
                "RunTs": r.get("RunTs"),
                "Version": r.get("Version"),
                "YdbSumMeans": _num(r.get("YdbSumMeans")),
            }
            for r in focus_slice
        ],
        "YdbSumMeans",
    )

    # Cross suite on same DbAlias
    by_suite: dict[str, list[dict[str, Any]]] = {}
    for r in filtered:
        if str(r.get("DbAlias") or "") != str(db):
            continue
        by_suite.setdefault(str(r.get("Suite") or ""), []).append(r)
    cross_suite: list[dict[str, Any]] = []
    for sname, lst in sorted(by_suite.items()):
        lst = sorted(lst, key=ts_key)
        series = [
            {
                "RunTs": r.get("RunTs"),
                "Version": str(r.get("Version") or "")[:7],
                "FailCount": _num(r.get("FailCount")),
                "YdbSumMeans": _num(r.get("YdbSumMeans")),
            }
            for r in lst
        ]
        fj = _largest_metric_step(
            [{"RunTs": x["RunTs"], "Version": x["Version"], "FailCount": x["FailCount"]} for x in series],
            "FailCount",
        )
        cross_suite.append(
            {
                "suite": sname,
                "n": len(series),
                "largest_fail_step": fj,
                "runs": series[-6:],
            }
        )

    # Peer DbAlias for same Suite
    peer_dbs: list[dict[str, Any]] = []
    by_db: dict[str, list[dict[str, Any]]] = {}
    for r in filtered:
        if suite and str(r.get("Suite") or "") != suite:
            continue
        d = str(r.get("DbAlias") or "")
        if d == str(db):
            continue
        by_db.setdefault(d, []).append(r)
    for d, lst in sorted(by_db.items()):
        lst = sorted(lst, key=ts_key)
        last = lst[-1]
        fj = _largest_metric_step(
            [
                {
                    "RunTs": r.get("RunTs"),
                    "Version": r.get("Version"),
                    "FailCount": _num(r.get("FailCount")),
                }
                for r in lst
            ],
            "FailCount",
        )
        peer_dbs.append(
            {
                "DbAlias": d,
                "n_runs_in_window": len(lst),
                "latest": {
                    "RunTs": last.get("RunTs"),
                    "Version": last.get("Version"),
                    "FailCount": last.get("FailCount"),
                    "YdbSumMeans": _num(last.get("YdbSumMeans")),
                },
                "largest_fail_step": fj,
            }
        )

    edge_hint = None
    if fail_jump and focus_slice and fail_jump.get("from_ts") == focus_slice[0].get("RunTs"):
        edge_hint = (
            "largest fail step starts at the first point in the dig window — "
            "re-run dig-runs with larger --days-before (60/90)"
        )

    slice_runs = [
        {
            "RunTs": r.get("RunTs"),
            "Version": r.get("Version"),
            "Suite": r.get("Suite"),
            "FailCount": r.get("FailCount"),
            "YdbSumMeans": _num(r.get("YdbSumMeans")),
            "Report": r.get("Report"),
        }
        for r in focus_slice
    ]
    from .baseline import select_baseline_from_slice_runs  # local import — avoid cycle at import time

    baseline_candidate = select_baseline_from_slice_runs(
        slice_runs,
        metric="YdbSumMeans",
        jump=ydb_jump,
        focus_version=str(selection.get("focus_sha") or "")[:7] or None,
    )
    stable_plateau = find_olap_stable_plateau(slice_runs)
    last_stable = _olap_last_stable_before_focus(slice_runs)
    pr_window = build_olap_pr_window(
        slice_runs, fail_jump=fail_jump, ydb_jump=ydb_jump
    )
    return {
        "slice_runs": slice_runs,
        "largest_fail_step": fail_jump,
        "largest_ydb_step": ydb_jump,
        "stable_plateau": stable_plateau,
        "last_stable_before_focus": last_stable,
        "pr_window": pr_window,
        "baseline_candidate": baseline_candidate,
        "cross_suite": cross_suite,
        "peer_dbs": peer_dbs,
        "row_count": len(rows),
        "slice_count": len(focus_slice),
        "window_edge_hint": edge_hint,
    }


def summarize_dig(
    *,
    kind: str,
    rows: list[dict[str, Any]],
    selection: dict[str, Any],
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "kind": kind,
        "selection": selection,
        "meta": meta or {},
        "summary": {},
        "note": (
            "Facts from perfomance mart (neighbors: other run_type/suites + peer "
            "clusters, same branch). Agent interprets correlations; widen window if edged."
        ),
    }
    if kind == "tpcc":
        out["summary"] = summarize_tpcc_rows(rows, selection=selection)
    else:
        out["summary"] = summarize_olap_rows(rows, selection=selection)
    return out
