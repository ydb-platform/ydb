#!/usr/bin/env python3
"""Update one PR comment as each Run-tests shard finishes.

shard_count=1 never calls this. The comment is identified by a stable HTML
marker. Concurrent shards retry when the comment ETag no longer matches, so
one update cannot drop another shard's result.

ETA uses the earliest reported shard start and the time of this update:
n == 0 -> unknown, n == m -> done, otherwise elapsed * (m - n) / n.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, urlsplit, urlunsplit

STATE_BEGIN = "<!-- shard-progress-state"
STATE_END = "-->"
MAX_FAILURE_NAMES = 12
RETRY_LIMIT = 6


class Conflict(Exception):
    """The comment changed between read and write."""


class CommentRecord(Protocol):
    id: int
    body: str
    etag: str


class CommentStore(Protocol):
    def list_marker(self, header: str) -> list[CommentRecord]:
        ...

    def get(self, comment_id: int) -> CommentRecord:
        ...

    def create(self, body: str) -> CommentRecord:
        ...

    def update(self, comment_id: int, body: str, etag: str) -> None:
        ...

    def delete(self, comment_id: int) -> None:
        ...


def eta_label(received: int, total: int, elapsed_seconds: float | None) -> str:
    """Remaining time from elapsed * (m - n) / n.

    n == 0 (or no clock) is unknown. n == m is done.
    """
    if received <= 0 or total <= 0 or elapsed_seconds is None:
        return "unknown"
    if received >= total:
        return "done"
    remaining = float(elapsed_seconds) * float(total - received) / float(received)
    return format_duration(remaining)


def format_duration(seconds: float) -> str:
    whole = max(0, int(round(seconds)))
    if whole < 60:
        return f"{whole}s"
    minutes, _sec = divmod(whole, 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def parse_time(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def marker(run_id: str, preset: str) -> str:
    return f"<!-- shard-progress run={run_id} preset={preset} -->"


_MARKER_RE = re.compile(r"^<!-- shard-progress run=(\S+) preset=(\S+) -->")


def marker_parts(body: str) -> tuple[str, str] | None:
    first = (body or "").split("\n", 1)[0].strip()
    match = _MARKER_RE.match(first)
    if not match:
        return None
    return match.group(1), match.group(2)


COUNT_KEYS = ("tests", "passed", "errors", "failed", "skipped", "muted")
_STATUS_TO_COUNT = {
    "PASSED": "passed",
    "FAILED": "failed",
    "ERROR": "errors",
    "SKIPPED": "skipped",
    "MUTE": "muted",
}
_STATUS_TO_ANCHOR = {
    "PASSED": "PASS",
    "FAILED": "FAIL",
    "ERROR": "ERROR",
    "SKIPPED": "SKIP",
    "MUTE": "MUTE",
}


def empty_counts() -> dict[str, int]:
    return {key: 0 for key in COUNT_KEYS}


def empty_state(run_id: str, preset: str, target: str, total: int) -> dict[str, Any]:
    return {
        "run_id": str(run_id),
        "preset": preset,
        "target": target,
        "total": int(total),
        "run_url": "",
        "started_at": "",
        "combined_url": "",
        "try_urls": {},
        "shards": {},
    }


def counts_from_report(report: dict[str, Any] | None) -> dict[str, int]:
    """One row of the TESTS / PASSED / FAILED table from a ya report.json."""
    found = empty_counts()
    if not report:
        return found
    for result in report.get("results") or []:
        if not isinstance(result, dict) or not result.get("status"):
            continue
        found["tests"] += 1
        bucket = _STATUS_TO_COUNT.get(str(result.get("status") or ""), "passed")
        found[bucket] += 1
    return found


def result_key(result: dict[str, Any]) -> tuple[str, ...]:
    # uid is the suite/node id and repeats across tests in one report.
    return (
        str(result.get("path") or ""),
        str(result.get("name") or ""),
        str(result.get("subtest_name") or ""),
    )


def merge_reports(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Later tries overlay earlier ones. try_1 is the full set; retries are a subset."""
    if not reports:
        return None
    by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    merged: dict[str, Any] = {}
    for report in reports:
        if not report:
            continue
        merged = dict(report)
        for result in report.get("results") or []:
            if not isinstance(result, dict) or not result.get("status"):
                continue
            by_key[result_key(result)] = result
    if not by_key:
        return merged or None
    merged["results"] = list(by_key.values())
    return merged


def _try_index(path: Path) -> int:
    match = re.search(r"try_(\d+)$", path.parent.name)
    return int(match.group(1)) if match else 0


def iter_try_reports(directory: str) -> list[tuple[int, dict[str, Any], Path]]:
    if not directory:
        return []
    root = Path(directory)
    if not root.is_dir():
        return []
    found: list[tuple[int, dict[str, Any], Path]] = []
    for path in sorted(root.glob("try_*/report.json"), key=lambda item: _try_index(item)):
        report = load_report(str(path))
        if report:
            found.append((_try_index(path), report, path))
    return found


def load_try_reports(directory: str) -> list[dict[str, Any]]:
    return [report for _index, report, _path in iter_try_reports(directory)]


def load_tries(directory: str) -> dict[str, dict[str, Any]]:
    """Per-try counts plus report mtime, same clock comment-pr uses on main."""
    found: dict[str, dict[str, Any]] = {}
    for index, report, path in iter_try_reports(directory):
        row: dict[str, Any] = counts_from_report(report)
        try:
            row["finished_at"] = format_time(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc))
        except OSError:
            pass
        found[str(index)] = row
    return found


def try_counts_from_dir(directory: str) -> dict[str, dict[str, int]]:
    return {
        key: {name: int(row.get(name) or 0) for name in COUNT_KEYS}
        for key, row in load_tries(directory).items()
    }


def try_report_urls(url: str) -> list[str]:
    """Expand .../try_3/report.json to try_1..try_3 so retries are not the only source."""
    if not url:
        return []
    match = re.search(r"/try_(\d+)/report\.json(?:\?.*)?$", url)
    if not match:
        return [url]
    last = int(match.group(1))
    return [re.sub(r"/try_\d+/report\.json", f"/try_{index}/report.json", url, count=1) for index in range(1, last + 1)]


def resolve_report(*, reports_dir: str = "", report_path: str = "") -> dict[str, Any] | None:
    reports = load_try_reports(reports_dir)
    if reports:
        return merge_reports(reports)
    return load_report(report_path)


def sum_counts(state: dict[str, Any]) -> dict[str, int]:
    found = empty_counts()
    for row in (state.get("shards") or {}).values():
        if not isinstance(row, dict):
            continue
        counts = row.get("counts") or {}
        for key in COUNT_KEYS:
            found[key] += int(counts.get(key) or 0)
    return found


def href_url(url: str) -> str:
    """Encode the path so spaces in github.workflow survive markdown and HTTP."""
    if not url:
        return url
    parts = urlsplit(url)
    if not parts.scheme:
        return url
    return urlunsplit(
        (parts.scheme, parts.netloc, quote(parts.path, safe="/%"), parts.query, parts.fragment)
    )


def report_url_for(state: dict[str, Any]) -> str:
    combined = str(state.get("combined_url") or "")
    if combined:
        return combined
    for shard_id in sorted(state.get("shards") or {}, key=lambda item: int(item)):
        url = str((state["shards"][shard_id] or {}).get("report_url") or "")
        if url:
            return url
    return ""


def try_url_for(state: dict[str, Any], key: str, *, try_count: int = 1) -> str:
    urls = state.get("try_urls") or {}
    if isinstance(urls, dict) and urls.get(str(key)):
        return str(urls[str(key)])
    combined = str(state.get("combined_url") or "")
    if try_count <= 1:
        return combined or report_url_for(state)
    if combined.endswith("/ya-test.html"):
        return combined[: -len("/ya-test.html")] + f"/try_{key}/ya-test.html"
    if combined:
        return f"{combined.rstrip('/')}/try_{key}/ya-test.html"
    return ""


def format_comment_time(value: str) -> str:
    """Same `YYYY-MM-DD HH:MM:SS UTC` clock as comment-pr.py / test_ya."""
    if not value:
        return ""
    return parse_time(value).strftime("%Y-%m-%d %H:%M:%S UTC")


def event_line(color: str, when: str, text: str) -> str:
    stamp = format_comment_time(when)
    if stamp:
        return f":{color}_circle: `{stamp}` {text}"
    return f":{color}_circle: {text}"


def try_finished_at(state: dict[str, Any], key: str, fallback: str = "") -> str:
    latest = ""
    for row in (state.get("shards") or {}).values():
        if not isinstance(row, dict):
            continue
        tries = shard_tries(row)
        if key not in tries:
            continue
        candidate = str(tries[key].get("finished_at") or row.get("finished_at") or "")
        if not candidate:
            continue
        if not latest or parse_time(candidate) > parse_time(latest):
            latest = candidate
    return latest or fallback


def _count_cell(value: int, url: str, anchor: str) -> str:
    if not value:
        return "0"
    encoded = href_url(url)
    if not encoded:
        return str(value)
    suffix = f"#{anchor}" if anchor else ""
    return f"[{value}]({encoded}{suffix})"


def _counts_cells(counts: dict[str, int], url: str, *, retry: bool = False) -> list[str]:
    tests = int(counts.get("tests") or 0)
    tests_cell = _count_cell(tests, url, "")
    if retry and tests and not url:
        tests_cell = f"{tests} (only retried tests)"
    elif retry and tests and url:
        tests_cell = f"{_count_cell(tests, url, '')} (only retried tests)"
    return [
        tests_cell,
        _count_cell(int(counts.get("passed") or 0), url, "PASS"),
        _count_cell(int(counts.get("errors") or 0), url, "ERROR"),
        _count_cell(int(counts.get("failed") or 0), url, "FAIL"),
        _count_cell(int(counts.get("skipped") or 0), url, "SKIP"),
        _count_cell(int(counts.get("muted") or 0), url, "MUTE"),
    ]


def shard_tries(row: dict[str, Any]) -> dict[str, dict[str, int]]:
    tries = row.get("tries") or {}
    if isinstance(tries, dict) and tries:
        return {str(key): dict(value) for key, value in tries.items() if isinstance(value, dict)}
    counts = row.get("counts")
    if isinstance(counts, dict) and any(int(counts.get(key) or 0) for key in COUNT_KEYS):
        return {"1": dict(counts)}
    return {}


def sum_try_counts(state: dict[str, Any]) -> dict[str, dict[str, int]]:
    found: dict[str, dict[str, int]] = {}
    for row in (state.get("shards") or {}).values():
        if not isinstance(row, dict):
            continue
        for key, counts in shard_tries(row).items():
            bucket = found.setdefault(key, empty_counts())
            for name in COUNT_KEYS:
                bucket[name] += int(counts.get(name) or 0)
    return found


def render_try_table(counts: dict[str, int], url: str, *, retry: bool = False) -> list[str]:
    return [
        "| TESTS | PASSED | ERRORS | FAILED | SKIPPED | MUTED |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
        "| " + " | ".join(_counts_cells(counts, url, retry=retry)) + " |",
        "",
    ]


def render_counts_table(state: dict[str, Any]) -> list[str]:
    tries = sum_try_counts(state)
    keys = sorted(tries, key=lambda item: int(item))
    if not keys:
        return render_try_table(sum_counts(state), report_url_for(state))
    last = keys[-1]
    return render_try_table(tries[last], try_url_for(state, last, try_count=len(keys)), retry=int(last) > 1)


def headline(state: dict[str, Any]) -> str:
    status = overall_status(state)
    received = received_count(state)
    total = int(state.get("total") or 0)
    if status == "running":
        return f"Tests still running ({received}/{total} shards)."
    if status == "failure":
        return "Some tests failed, follow the links below."
    return "Tests successful."


def last_try_failed(row: dict[str, Any]) -> bool:
    """True when this shard's latest try still has FAIL/ERROR. Earlier tries do not count."""
    tries = shard_tries(row)
    if tries:
        last = tries[max(tries, key=lambda key: int(key))]
        return bool(int(last.get("failed") or 0) or int(last.get("errors") or 0))
    if str(row.get("result") or "") not in ("", "success"):
        return True
    counts = row.get("counts") or {}
    return bool(int(counts.get("failed") or 0) or int(counts.get("errors") or 0))


def seen_failures(state: dict[str, Any]) -> bool:
    """Red as soon as any received shard's last try failed. Do not wait for the rest."""
    return any(isinstance(row, dict) and last_try_failed(row) for row in (state.get("shards") or {}).values())


def running_event(state: dict[str, Any], now: str) -> str:
    received = received_count(state)
    total = int(state.get("total") or 0)
    text = f"Tests still running ({received}/{total} shards)."
    eta = eta_label(received, total, elapsed_seconds(state, now))
    if eta not in ("unknown", "done"):
        text += f" **ETA:** {eta}"
    color = "red" if seen_failures(state) else "yellow"
    return event_line(color, now, text)


def render_timeline(state: dict[str, Any], now: str) -> list[str]:
    """Rebuild the test_ya / comment-pr log: circles, UTC stamps, last try open."""
    lines: list[str] = []
    preset = _md(str(state.get("preset") or ""))
    started = str(state.get("started_at") or now)
    run_url = str(state.get("run_url") or "")
    start = f"Run-tests `{preset}` has started."
    if run_url:
        start += f" [Run]({run_url})"
    lines.append(event_line("white", started, start))
    tries = sum_try_counts(state)
    keys = sorted(tries, key=lambda item: int(item))
    status = overall_status(state)
    if not keys:
        if status == "running":
            lines.append(running_event(state, now))
        elif status == "failure":
            lines.append(event_line("red", now, "Some tests failed, follow the links below."))
        else:
            lines.append(event_line("green", now, "Tests successful."))
        lines.append("")
        lines.extend(render_try_table(sum_counts(state), report_url_for(state)))
        return lines
    last = keys[-1]
    for key in keys:
        when = try_finished_at(state, key, now)
        url = try_url_for(state, key, try_count=len(keys))
        table = render_try_table(tries[key], url, retry=int(key) > 1)
        if key != last:
            lines.append(
                event_line(
                    "yellow",
                    when,
                    "Some tests failed, follow the links below. Going to retry failed tests...",
                )
            )
            lines.append("")
            lines.append("<details>")
            lines.append("")
            lines.extend(table)
            lines.append("</details>")
            lines.append("")
            continue
        if status == "running":
            lines.append(running_event(state, now))
        elif status == "failure":
            lines.append(event_line("red", when or now, "Some tests failed, follow the links below."))
        else:
            lines.append(event_line("green", when or now, "Tests successful."))
        lines.append("")
        lines.extend(table)
        if status == "success":
            lines.append(event_line("green", when or now, "Build successful."))
    return lines


def _min_time(current: str, candidate: str) -> str:
    if not candidate:
        return current
    if not current:
        return candidate
    return candidate if parse_time(candidate) < parse_time(current) else current


def merge_states(states: list[dict[str, Any]]) -> dict[str, Any]:
    """Union shard rows. The same shard id keeps the later finish."""
    merged: dict[str, Any] | None = None
    for state in states:
        if not state:
            continue
        if merged is None:
            merged = {
                "run_id": str(state.get("run_id") or ""),
                "preset": str(state.get("preset") or ""),
                "target": str(state.get("target") or ""),
                "total": int(state.get("total") or 0),
                "run_url": str(state.get("run_url") or ""),
                "started_at": str(state.get("started_at") or ""),
                "combined_url": str(state.get("combined_url") or ""),
                "try_urls": dict(state.get("try_urls") or {}),
                "shards": {},
            }
        merged["total"] = max(int(merged["total"]), int(state.get("total") or 0))
        if not merged["run_url"]:
            merged["run_url"] = str(state.get("run_url") or "")
        if state.get("combined_url"):
            merged["combined_url"] = str(state.get("combined_url") or "")
        if state.get("try_urls"):
            merged.setdefault("try_urls", {})
            merged["try_urls"].update(state["try_urls"])
        merged["started_at"] = _min_time(str(merged["started_at"]), str(state.get("started_at") or ""))
        for shard_id, row in (state.get("shards") or {}).items():
            key = str(shard_id)
            previous = merged["shards"].get(key)
            if previous is None or str(row.get("finished_at") or "") >= str(previous.get("finished_at") or ""):
                merged["shards"][key] = dict(row)
            merged["started_at"] = _min_time(str(merged["started_at"]), str(row.get("started_at") or ""))
    if merged is None:
        raise ValueError("no shard progress state to merge")
    return merged


def apply_shard(
    state: dict[str, Any],
    *,
    shard_id: int,
    result: str,
    started_at: str,
    finished_at: str,
    job_url: str,
    log_prefix: str,
    failed_tests: list[str],
    run_url: str,
    build: str = "",
    tests: str = "",
    counts: dict[str, int] | None = None,
    tries: dict[str, dict[str, Any]] | None = None,
    report_url: str = "",
    report_json_url: str = "",
) -> dict[str, Any]:
    updated = merge_states([state])
    if run_url:
        updated["run_url"] = run_url
    updated["started_at"] = _min_time(str(updated["started_at"]), started_at)
    updated["shards"][str(shard_id)] = {
        "result": result,
        "started_at": started_at,
        "finished_at": finished_at,
        "job_url": job_url,
        "log_prefix": log_prefix,
        "failed_tests": list(failed_tests),
        "build": build,
        "tests": tests,
        "counts": dict(counts or empty_counts()),
        "tries": {str(key): dict(value) for key, value in (tries or {}).items()},
        "report_url": report_url,
        "report_json_url": report_json_url,
    }
    return updated


def elapsed_seconds(state: dict[str, Any], now: str) -> float | None:
    started = str(state.get("started_at") or "")
    if not started or not now:
        return None
    return (parse_time(now) - parse_time(started)).total_seconds()


def list_run_jobs(repository: str, token: str, run_id: str) -> list[dict[str, Any]]:
    """Every job of one workflow run, following Link rel=next."""
    jobs: list[dict[str, Any]] = []
    url = f"https://api.github.com/repos/{repository}/actions/runs/{run_id}/jobs?per_page=100"
    seen: set[str] = set()
    while url and url not in seen:
        seen.add(url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "ydb-shard-progress",
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
            link = str(response.headers.get("Link", ""))
        if not isinstance(payload, dict):
            break
        for job in payload.get("jobs") or []:
            if isinstance(job, dict):
                jobs.append(job)
        url = next_link(link)
    return jobs


def job_url_for_shard(jobs: list[dict[str, Any]], preset: str, shard_id: int) -> str:
    """Match the shard id as its own token. ``shard 1`` must not hit ``shard 10``."""
    suffix = f"shard {shard_id}"
    for job in jobs:
        name = str(job.get("name") or "")
        if preset in name and name.endswith(suffix):
            return str(job.get("html_url") or "")
    return ""


def aggregate_check_states(rows: list[dict[str, str]]) -> tuple[str | None, str | None]:
    """Commit-status pair for one preset.

    Build is success only when every shard recorded a successful ya make.
    Tests are success only when every shard ran tests and they passed.
    Tests are failure only when a shard actually failed tests. A shard that
    never got that far leaves tests unset so a build failure is not also
    reported as a test failure.
    """
    if not rows:
        return None, None
    builds = [str(row.get("build") or "") for row in rows]
    tests = [str(row.get("tests") or "") for row in rows]
    if all(item == "success" for item in builds):
        build_state: str | None = "success"
    else:
        build_state = "failure"
    if any(item == "failure" for item in tests):
        test_state: str | None = "failure"
    elif all(item == "success" for item in tests):
        test_state = "success"
    else:
        test_state = None
    return build_state, test_state


def rows_for_preset(
    states: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    preset: str,
) -> list[dict[str, str]]:
    """Shard outcome rows, with a failed job standing in when its file is missing."""
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    prefix = f"Test {preset} shard "
    for state in states:
        if str(state.get("preset") or "") != preset:
            continue
        for shard_id, row in (state.get("shards") or {}).items():
            if not isinstance(row, dict):
                continue
            seen.add(str(shard_id))
            rows.append({"build": str(row.get("build") or ""), "tests": str(row.get("tests") or "")})
    for job in jobs:
        name = str(job.get("name") or "")
        if not name.startswith(prefix):
            continue
        shard_id = name[len(prefix):].strip()
        if shard_id in seen:
            continue
        # A green job with no file still passed its own test step. A red job
        # with no file is not proof that ya make failed, so leave it unset and
        # let the workflow gate fail the run.
        if job.get("conclusion") == "success":
            rows.append({"build": "success", "tests": "success"})
    return rows


def presets_needing_build_failure(
    jobs: list[dict[str, Any]],
    states: list[dict[str, Any]],
    presets: list[str],
) -> list[str]:
    """Matrix presets whose Build and test job died before any shard or single-job status.

    ``Run the single job`` posts build_*/test_* itself. Do not overwrite those.
    """
    missing: list[str] = []
    for preset in presets:
        if rows_for_preset(states, jobs, preset):
            continue
        job = next((item for item in jobs if str(item.get("name") or "") == f"Build and test {preset}"), None)
        if not job or job.get("conclusion") != "failure":
            continue
        steps = job.get("steps") or []
        single = next((step for step in steps if str(step.get("name") or "") == "Run the single job"), None)
        if single and single.get("conclusion") not in (None, "skipped"):
            continue
        missing.append(preset)
    return missing


def load_state_files(directory: str) -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    if not directory:
        return states
    root = Path(directory)
    if not root.is_dir():
        return states
    for path in sorted(root.glob("**/*.json")):
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(parsed, dict) and parsed.get("shards"):
            states.append(parsed)
    return states


def received_count(state: dict[str, Any]) -> int:
    return len(state.get("shards") or {})


def overall_status(state: dict[str, Any]) -> str:
    total = int(state.get("total") or 0)
    shards = state.get("shards") or {}
    if len(shards) < total or total <= 0:
        return "running"
    if any(str(row.get("result")) != "success" for row in shards.values()):
        return "failure"
    return "success"


def failed_test_names(report: dict[str, Any], limit: int = MAX_FAILURE_NAMES) -> list[str]:
    names: list[str] = []
    for result in report.get("results") or []:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "")
        if status not in ("FAILED", "ERROR"):
            continue
        path = str(result.get("path") or "")
        name = str(result.get("name") or "")
        subtest = str(result.get("subtest_name") or "")
        if subtest:
            test_name = f"{name}.{subtest}" if name else subtest
        else:
            test_name = name
        full_name = f"{path}/{test_name}" if path and test_name else (test_name or path)
        if full_name:
            names.append(full_name)
        if len(names) >= limit:
            break
    return names


def _md(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_comment(state: dict[str, Any], now: str) -> str:
    lines = [
        marker(str(state.get("run_id") or ""), str(state.get("preset") or "")),
        "",
    ]
    lines.extend(render_timeline(state, now))
    payload = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    lines.append(STATE_BEGIN)
    lines.append(payload)
    lines.append(STATE_END)
    return "\n".join(lines) + "\n"


def parse_state(body: str) -> dict[str, Any] | None:
    start = body.find(STATE_BEGIN)
    if start < 0:
        return None
    start = body.find("\n", start)
    if start < 0:
        return None
    end = body.find(STATE_END, start)
    if end < 0:
        return None
    raw = body[start:end].strip()
    if not raw:
        return None
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("shard progress state is not an object")
    return parsed


def comment_body_for(
    existing: list[dict[str, Any] | None],
    event_state: dict[str, Any],
    now: str,
) -> str:
    states = [state for state in existing if state]
    states.append(event_state)
    merged = merge_states(states)
    return render_comment(merged, now)


class RestComment:
    def __init__(self, comment_id: int, body: str, etag: str) -> None:
        self.id = comment_id
        self.body = body
        self.etag = etag


def next_link(link_header: str) -> str:
    """Return the URL marked rel=next in a GitHub Link header, or ''."""
    for part in (link_header or "").split(","):
        bits = [bit.strip() for bit in part.split(";")]
        if not bits or not bits[0].startswith("<") or not bits[0].endswith(">"):
            continue
        rels = {bit for bit in bits[1:]}
        if 'rel="next"' in rels or "rel=next" in rels:
            return bits[0][1:-1]
    return ""


class GithubCommentStore:
    def __init__(self, token: str, repository: str, pr_number: int) -> None:
        self._token = token
        self._repository = repository
        self._pr_number = pr_number

    def list_marker(self, header: str) -> list[RestComment]:
        found: list[RestComment] = []
        url = f"https://api.github.com/repos/{self._repository}/issues/{self._pr_number}/comments?per_page=100"
        while url:
            status, headers, payload = self._request("GET", url)
            if status != 200 or not isinstance(payload, list):
                raise RuntimeError(f"list comments failed: HTTP {status}")
            for item in payload:
                body = str(item.get("body") or "")
                if body.startswith(header):
                    found.append(RestComment(int(item["id"]), body, ""))
            url = next_link(headers.get("Link", ""))
        return found

    def get(self, comment_id: int) -> RestComment:
        status, headers, payload = self._request(
            "GET",
            f"https://api.github.com/repos/{self._repository}/issues/comments/{comment_id}",
        )
        if status != 200 or not isinstance(payload, dict):
            raise RuntimeError(f"get comment {comment_id} failed: HTTP {status}")
        return RestComment(comment_id, str(payload.get("body") or ""), headers.get("ETag", ""))

    def create(self, body: str) -> RestComment:
        status, _headers, payload = self._request(
            "POST",
            f"https://api.github.com/repos/{self._repository}/issues/{self._pr_number}/comments",
            {"body": body},
        )
        if status not in (200, 201) or not isinstance(payload, dict):
            raise RuntimeError(f"create comment failed: HTTP {status}")
        return RestComment(int(payload["id"]), body, "")

    def update(self, comment_id: int, body: str, etag: str) -> None:
        # Do not send If-Match: issue-comment PATCH answers HTTP 400.
        status, _headers, payload = self._request(
            "PATCH",
            f"https://api.github.com/repos/{self._repository}/issues/comments/{comment_id}",
            {"body": body},
        )
        if status == 412:
            raise Conflict(str(comment_id))
        if status != 200:
            raise RuntimeError(f"update comment {comment_id} failed: HTTP {status}: {payload}")

    def delete(self, comment_id: int) -> None:
        status, _headers, _payload = self._request(
            "DELETE",
            f"https://api.github.com/repos/{self._repository}/issues/comments/{comment_id}",
        )
        if status not in (204, 404):
            raise RuntimeError(f"delete comment {comment_id} failed: HTTP {status}")

    def _request(
        self,
        method: str,
        url: str,
        payload: dict[str, Any] | None = None,
        etag: str | None = None,
    ) -> tuple[int, dict[str, str], Any]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "ydb-shard-progress",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        if etag:
            headers["If-Match"] = etag
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request) as response:
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw) if raw else None
                return response.status, dict(response.headers), parsed
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                parsed = raw
            return exc.code, dict(exc.headers), parsed


def sync_comment(store: CommentStore, header: str, state: dict[str, Any], now: str) -> str:
    """Write one comment per build. Older runs of the same preset are removed."""
    body = ""
    preset = str(state.get("preset") or "")
    run_id = str(state.get("run_id") or "")
    for _attempt in range(RETRY_LIMIT):
        matches = store.list_marker(header)
        if not matches:
            body = render_comment(state, now)
            created = store.create(body)
            matches = store.list_marker(header)
            if len(matches) <= 1:
                _cleanup_comments(store, matches, created.id, preset, run_id)
                return body
        canonical = min(matches, key=lambda item: item.id)
        fresh = store.get(canonical.id)
        # list_marker can be stale by the time get() returns. Merge the fresh
        # body, not the listed copy of the same comment, or a shard that landed
        # in between is overwritten without an ETag conflict.
        parsed: list[dict[str, Any] | None] = []
        for item in matches:
            if item.id == fresh.id:
                parsed.append(parse_state(fresh.body))
            else:
                parsed.append(parse_state(item.body))
        parsed.append(state)
        merged = merge_states([item for item in parsed if item])
        body = render_comment(merged, now)
        if fresh.body == body:
            _cleanup_comments(store, matches, fresh.id, preset, run_id)
            return body
        try:
            store.update(fresh.id, body, fresh.etag)
        except Conflict:
            time.sleep(1)
            continue
        _cleanup_comments(store, matches, fresh.id, preset, run_id)
        return body
    raise RuntimeError("shard progress comment kept conflicting; retries exhausted")


def _cleanup_comments(
    store: CommentStore,
    matches: list[CommentRecord],
    keep_id: int,
    preset: str,
    run_id: str,
) -> None:
    extra = [item for item in matches if item.id != keep_id]
    if preset:
        for item in store.list_marker("<!-- shard-progress "):
            if item.id == keep_id:
                continue
            parts = marker_parts(item.body)
            if parts and parts[1] == preset and parts[0] != run_id:
                extra.append(item)
    seen: set[int] = set()
    for item in extra:
        if item.id in seen:
            continue
        seen.add(item.id)
        try:
            store.delete(item.id)
        except RuntimeError as exc:
            print(f"warning: {exc}", file=sys.stderr)


def load_report(path: str) -> dict[str, Any] | None:
    if not path or not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as handle:
        report = json.load(handle)
    return report if isinstance(report, dict) else None


def _load_failed_tests(path: str) -> list[str]:
    report = load_report(path)
    return failed_test_names(report) if report else []


def fetch_report(url: str) -> dict[str, Any] | None:
    if not url:
        return None
    try:
        with urllib.request.urlopen(href_url(url), timeout=30) as response:
            payload = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError, TimeoutError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def fetch_merged_report(url: str) -> dict[str, Any] | None:
    reports = [item for item in (fetch_report(item) for item in try_report_urls(url)) if item]
    return merge_reports(reports)


def report_json_url_for_try(url: str, try_index: int) -> str:
    if not url:
        return ""
    if re.search(r"/try_\d+/report\.json", url):
        return re.sub(r"/try_\d+/report\.json", f"/try_{try_index}/report.json", url, count=1)
    return url if try_index == 1 else ""


def collect_reports(
    state: dict[str, Any],
    *,
    local_path: str,
    local_json_url: str = "",
    reports_dir: str = "",
) -> list[dict[str, Any]]:
    """Local merged tries first, then every other shard's public reports."""
    found: list[dict[str, Any]] = []
    local = resolve_report(reports_dir=reports_dir, report_path=local_path)
    if local:
        found.append(local)
    skip = set(try_report_urls(local_json_url)) if local_json_url else set()
    if local_json_url:
        skip.add(local_json_url)
    for row in (state.get("shards") or {}).values():
        if not isinstance(row, dict):
            continue
        url = str(row.get("report_json_url") or "")
        if not url or url in skip:
            continue
        skip.update(try_report_urls(url))
        skip.add(url)
        remote = fetch_merged_report(url)
        if remote:
            found.append(remote)
    return found


def collect_try_reports(
    state: dict[str, Any],
    try_index: int,
    *,
    local_json_url: str = "",
    reports_dir: str = "",
) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if reports_dir:
        local = load_report(str(Path(reports_dir) / f"try_{try_index}" / "report.json"))
        if local:
            found.append(local)
    skip = {report_json_url_for_try(local_json_url, try_index)} if local_json_url else set()
    skip.discard("")
    for row in (state.get("shards") or {}).values():
        if not isinstance(row, dict):
            continue
        url = report_json_url_for_try(str(row.get("report_json_url") or ""), try_index)
        if not url or url in skip:
            continue
        skip.add(url)
        remote = fetch_report(url)
        if remote:
            found.append(remote)
    return found


def write_try_htmls(
    directory: str,
    state: dict[str, Any],
    *,
    preset: str,
    local_json_url: str = "",
    reports_dir: str = "",
    combined_url: str = "",
) -> dict[str, str]:
    urls: dict[str, str] = {}
    base = combined_url.rstrip("/")
    if base.endswith("/ya-test.html"):
        base = base[: -len("/ya-test.html")]
    for key in sum_try_counts(state):
        reports = collect_try_reports(
            state,
            int(key),
            local_json_url=local_json_url,
            reports_dir=reports_dir,
        )
        if not reports:
            continue
        write_combined_html(str(Path(directory) / f"try_{key}" / "ya-test.html"), reports, preset=preset)
        if base:
            urls[str(key)] = f"{base}/try_{key}/ya-test.html"
    return urls


def _html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_combined_html(path: str, reports: list[dict[str, Any]], *, preset: str) -> None:
    """One HTML file with the same #FAIL / #PASS anchors as the single-job report."""
    by_anchor: dict[str, list[str]] = {anchor: [] for anchor in _STATUS_TO_ANCHOR.values()}
    by_anchor["ALL"] = []
    for report in reports:
        for result in report.get("results") or []:
            if not isinstance(result, dict) or not result.get("status"):
                continue
            status = str(result.get("status") or "")
            anchor = _STATUS_TO_ANCHOR.get(status, "PASS")
            path_name = str(result.get("path") or "")
            name = str(result.get("name") or "")
            sub = str(result.get("subtest_name") or "")
            if sub:
                name = f"{name}.{sub}" if name else sub
            full = f"{path_name}/{name}" if path_name and name else (name or path_name)
            links = result.get("links") if isinstance(result.get("links"), dict) else {}
            href = ""
            for key in ("log", "Log", "stderr"):
                raw = links.get(key)
                if isinstance(raw, list) and raw:
                    href = str(raw[0])
                    break
            label = _html_escape(full or "(unnamed)")
            item = f'<li><a href="{_html_escape(href)}">{label}</a></li>' if href else f"<li>{label}</li>"
            by_anchor[anchor].append(item)
            by_anchor["ALL"].append(item)
    nav = " · ".join(f'<a href="#{name}">{name}</a>' for name in ("ALL", "PASS", "ERROR", "FAIL", "SKIP", "MUTE"))
    sections = []
    for name in ("ALL", "PASS", "ERROR", "FAIL", "SKIP", "MUTE"):
        items = by_anchor.get(name) or []
        body = f"<ul>{''.join(items)}</ul>" if items else "<p>none</p>"
        sections.append(f'<section id="{name}"><h2>{name} ({len(items)})</h2>{body}</section>')
    html = (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        f"<title>Combined tests {_html_escape(preset)}</title>"
        "<style>body{font-family:sans-serif;margin:1.5rem}section{margin-top:2rem}</style>"
        "</head><body>"
        f"<h1>Combined tests `{_html_escape(preset)}`</h1><p>{nav}</p>"
        + "".join(sections)
        + "</body></html>\n"
    )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(html, encoding="utf-8")


def _event_state(args: argparse.Namespace) -> dict[str, Any]:
    base = empty_state(args.run_id, args.preset, args.target, args.shard_count)
    base["run_url"] = args.run_url
    if getattr(args, "combined_url", ""):
        base["combined_url"] = args.combined_url
    reports_dir = getattr(args, "reports_dir", "") or ""
    report = resolve_report(reports_dir=reports_dir, report_path=args.report)
    return apply_shard(
        base,
        shard_id=args.shard_id,
        result=args.result,
        started_at=args.started_at,
        finished_at=args.finished_at,
        job_url=args.job_url,
        log_prefix=args.log_prefix or f"shard_{args.shard_id}",
        failed_tests=failed_test_names(report) if report else [],
        run_url=args.run_url,
        build=args.build_result,
        tests=args.test_result,
        counts=counts_from_report(report),
        tries=load_tries(reports_dir),
        report_url=getattr(args, "report_url", "") or "",
        report_json_url=getattr(args, "report_json_url", "") or "",
    )


def render_shard_job_note(state: dict[str, Any], shard_id: int) -> str:
    """One job-summary line for this shard. The combined table is written later."""
    row = (state.get("shards") or {}).get(str(shard_id)) or {}
    counts = row.get("counts") or empty_counts()
    preset = _md(str(state.get("preset") or ""))
    return (
        f"### This shard only (`{preset}` shard {shard_id})\n"
        f"\n"
        f"Job result: **{row.get('result') or 'unknown'}**. "
        f"TESTS {int(counts.get('tests') or 0)}, "
        f"PASSED {int(counts.get('passed') or 0)}, "
        f"FAILED {int(counts.get('failed') or 0)}, "
        f"MUTED {int(counts.get('muted') or 0)}.\n"
        f"\n"
        f"The combined TESTS table is the PR comment and the `shard_result` job "
        f"after every shard finishes. Tables above are this job's tries only.\n"
    )


def visible_comment(body: str) -> str:
    """Drop the hidden JSON state so job summaries stay readable."""
    start = body.find(STATE_BEGIN)
    return body[:start].rstrip() + "\n" if start >= 0 else body


def _write_summary(path: str, body: str) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(body)


def _write_state(path: str, state: dict[str, Any]) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(state, ensure_ascii=False) + "\n", encoding="utf-8")


def _cmd_publish(args: argparse.Namespace) -> int:
    state = _event_state(args)
    if getattr(args, "combined_html", ""):
        preview = state
        token = os.environ.get("GITHUB_TOKEN", "")
        repository = os.environ.get("GITHUB_REPOSITORY", "")
        if args.pr and token and repository:
            try:
                store = GithubCommentStore(token, repository, int(args.pr))
                matches = store.list_marker(marker(args.run_id, args.preset))
                existing = [parse_state(item.body) for item in matches]
                preview = merge_states([item for item in existing if item] + [state])
            except (OSError, RuntimeError, urllib.error.URLError, json.JSONDecodeError, TimeoutError, ValueError):
                preview = state
        try_urls = write_try_htmls(
            args.combined_html,
            preview,
            preset=args.preset,
            local_json_url=getattr(args, "report_json_url", "") or "",
            reports_dir=getattr(args, "reports_dir", "") or "",
            combined_url=getattr(args, "combined_url", "") or "",
        )
        if try_urls:
            state["try_urls"] = try_urls
        if getattr(args, "combined_url", ""):
            state["combined_url"] = args.combined_url
    # Disk copy survives a comment API failure so finalize can still merge it.
    _write_state(args.state_output, state)
    _write_summary(args.summary_file, render_shard_job_note(state, args.shard_id))
    if not args.pr:
        print("No PR number; wrote the shard note only.", file=sys.stderr)
        return 0
    token = os.environ.get("GITHUB_TOKEN", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repository:
        print("GITHUB_TOKEN or GITHUB_REPOSITORY is empty; shard result is on disk.", file=sys.stderr)
        return 0 if args.state_output else 1
    try:
        store = GithubCommentStore(token, repository, int(args.pr))
        header = marker(args.run_id, args.preset)
        written = sync_comment(store, header, state, args.finished_at)
    except (OSError, RuntimeError, urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
        print(f"warning: comment update failed ({exc}); shard result is on disk.", file=sys.stderr)
        return 0 if args.state_output else 1
    sys.stdout.write(written)
    return 0


def _cmd_finalize(args: argparse.Namespace) -> int:
    states = [
        state for state in load_state_files(args.state_dir) if str(state.get("preset") or "") == args.preset
    ]
    if not args.pr:
        print("No PR number; nothing to finalize.", file=sys.stderr)
        return 0
    token = os.environ.get("GITHUB_TOKEN", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repository:
        print("GITHUB_TOKEN or GITHUB_REPOSITORY is empty.", file=sys.stderr)
        return 1
    store = GithubCommentStore(token, repository, int(args.pr))
    header = marker(args.run_id, args.preset)
    matches = store.list_marker(header)
    parsed = [parse_state(item.body) for item in matches]
    states.extend(item for item in parsed if item)
    if not states:
        print("No shard progress comment to finalize.", file=sys.stderr)
        return 0
    merged = merge_states(states)
    now = args.now or format_time(datetime.now(timezone.utc))
    written = render_comment(merged, now)
    heading = f"## Combined Run-tests `{_md(args.preset)}`\n\n"
    _write_summary(args.summary_file, heading + visible_comment(written) + "\n")
    try:
        written = sync_comment(store, header, merged, now)
    except (OSError, RuntimeError, urllib.error.URLError, json.JSONDecodeError, TimeoutError, ValueError) as exc:
        print(f"warning: comment update failed ({exc}); wrote the combined summary only.", file=sys.stderr)
        return 0
    return 0


def _cmd_job_url(args: argparse.Namespace) -> int:
    token = os.environ.get("GITHUB_TOKEN", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repository:
        return 0
    print(job_url_for_shard(list_run_jobs(repository, token, str(args.run_id)), args.preset, args.shard_id))
    return 0


def _cmd_list_jobs(args: argparse.Namespace) -> int:
    token = os.environ.get("GITHUB_TOKEN", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repository:
        print("[]")
        return 0
    jobs = list_run_jobs(repository, token, str(args.run_id))
    json.dump(jobs, sys.stdout)
    sys.stdout.write("\n")
    return 0


def _cmd_missing_build(args: argparse.Namespace) -> int:
    states = load_state_files(args.state_dir)
    jobs = json.loads(args.jobs.read_text(encoding="utf-8"))
    if not isinstance(jobs, list):
        raise ValueError("jobs file must be a JSON list")
    presets = [item for item in args.presets.split(",") if item]
    for preset in presets_needing_build_failure(jobs, states, presets):
        print(preset)
    return 0


def _cmd_aggregate_statuses(args: argparse.Namespace) -> int:
    states = load_state_files(args.state_dir)
    jobs = json.loads(args.jobs.read_text(encoding="utf-8"))
    if not isinstance(jobs, list):
        raise ValueError("jobs file must be a JSON list")
    build_state, test_state = aggregate_check_states(rows_for_preset(states, jobs, args.preset))
    print(f"build={build_state or ''}")
    print(f"tests={test_state or ''}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish incremental Run-tests shard progress.")
    sub = parser.add_subparsers(dest="command", required=True)

    publish = sub.add_parser("publish", help="Record one finished shard and update the PR comment.")
    publish.add_argument("--pr", default="")
    publish.add_argument("--run-id", required=True)
    publish.add_argument("--preset", required=True)
    publish.add_argument("--target", default="")
    publish.add_argument("--shard-id", type=int, required=True)
    publish.add_argument("--shard-count", type=int, required=True)
    publish.add_argument("--result", required=True)
    publish.add_argument("--started-at", required=True)
    publish.add_argument("--finished-at", required=True)
    publish.add_argument("--job-url", default="")
    publish.add_argument("--run-url", default="")
    publish.add_argument("--log-prefix", default="")
    publish.add_argument("--report", default="")
    publish.add_argument("--reports-dir", default="")
    publish.add_argument("--report-url", default="")
    publish.add_argument("--report-json-url", default="")
    publish.add_argument("--combined-html", default="")
    publish.add_argument("--combined-url", default="")
    publish.add_argument("--summary-file", default="")
    publish.add_argument("--state-output", default="")
    publish.add_argument("--build-result", default="")
    publish.add_argument("--test-result", default="")
    publish.set_defaults(func=_cmd_publish)

    finalize = sub.add_parser("finalize", help="Merge marker comments into one final summary.")
    finalize.add_argument("--pr", default="")
    finalize.add_argument("--run-id", required=True)
    finalize.add_argument("--preset", required=True)
    finalize.add_argument("--now", default="")
    finalize.add_argument("--summary-file", default="")
    finalize.add_argument("--state-dir", default="")
    finalize.set_defaults(func=_cmd_finalize)

    jobs_cmd = sub.add_parser("list-jobs", help="Print every job of a workflow run as JSON.")
    jobs_cmd.add_argument("--run-id", required=True)
    jobs_cmd.set_defaults(func=_cmd_list_jobs)

    job_url = sub.add_parser("job-url", help="HTML URL of one shard job, scanning every page.")
    job_url.add_argument("--run-id", required=True)
    job_url.add_argument("--preset", required=True)
    job_url.add_argument("--shard-id", type=int, required=True)
    job_url.set_defaults(func=_cmd_job_url)

    statuses = sub.add_parser("aggregate-statuses", help="Print build and test commit states for one preset.")
    statuses.add_argument("--preset", required=True)
    statuses.add_argument("--state-dir", required=True)
    statuses.add_argument("--jobs", required=True, type=Path)
    statuses.set_defaults(func=_cmd_aggregate_statuses)

    missing = sub.add_parser("missing-build", help="Presets that died before any status was posted.")
    missing.add_argument("--state-dir", default="")
    missing.add_argument("--jobs", required=True, type=Path)
    missing.add_argument("--presets", required=True, help="Comma-separated preset names from the PR-check matrix")
    missing.set_defaults(func=_cmd_missing_build)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
