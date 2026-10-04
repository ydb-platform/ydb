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
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Protocol

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


def empty_state(run_id: str, preset: str, target: str, total: int) -> dict[str, Any]:
    return {
        "run_id": str(run_id),
        "preset": preset,
        "target": target,
        "total": int(total),
        "run_url": "",
        "started_at": "",
        "shards": {},
    }


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
                "shards": {},
            }
        merged["total"] = max(int(merged["total"]), int(state.get("total") or 0))
        if not merged["run_url"]:
            merged["run_url"] = str(state.get("run_url") or "")
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
    }
    return updated


def elapsed_seconds(state: dict[str, Any], now: str) -> float | None:
    started = str(state.get("started_at") or "")
    if not started or not now:
        return None
    return (parse_time(now) - parse_time(started)).total_seconds()


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
    total = int(state.get("total") or 0)
    received = received_count(state)
    eta = eta_label(received, total, elapsed_seconds(state, now))
    status = overall_status(state)
    lines = [
        marker(str(state.get("run_id") or ""), str(state.get("preset") or "")),
        f"### Sharded Run-tests `{_md(str(state.get('preset') or ''))}`",
        "",
        f"**Progress:** {received}/{total}",
        f"**ETA:** {eta}",
        f"**Status:** {status}",
    ]
    target = str(state.get("target") or "")
    if target:
        lines.append(f"**Target:** `{_md(target)}`")
    run_url = str(state.get("run_url") or "")
    if run_url:
        lines.append(f"**Run:** {run_url}")
    lines.append("")

    failures: list[str] = []
    for shard_id in sorted(state.get("shards") or {}, key=lambda item: int(item)):
        row = state["shards"][shard_id]
        if str(row.get("result")) == "success":
            continue
        tests = [str(name) for name in row.get("failed_tests") or []]
        shown = ", ".join(f"`{_md(name)}`" for name in tests[:MAX_FAILURE_NAMES]) or "see job log"
        extra = len(tests) - MAX_FAILURE_NAMES
        if extra > 0:
            shown += f" … (+{extra})"
        link = str(row.get("job_url") or "")
        prefix = str(row.get("log_prefix") or f"shard_{shard_id}")
        link_text = f"[shard {shard_id}]({link})" if link else f"shard {shard_id}"
        failures.append(
            f"- shard {shard_id} **{row.get('result')}** — {shown} — {link_text} — logs `{_md(prefix)}`"
        )
    if failures:
        lines.append("Failures so far:")
        lines.extend(failures)
    elif status == "running":
        lines.append("Failures so far: none yet")
    else:
        lines.append("Failures so far: none")
    lines.append("")
    lines.append("| Shard | Result | Job | Logs |")
    lines.append("| ---: | --- | --- | --- |")
    for shard_id in sorted(state.get("shards") or {}, key=lambda item: int(item)):
        row = state["shards"][shard_id]
        link = str(row.get("job_url") or "")
        job = f"[shard {shard_id}]({link})" if link else f"shard {shard_id}"
        prefix = str(row.get("log_prefix") or f"shard_{shard_id}")
        lines.append(f"| {shard_id} | {row.get('result')} | {job} | `{_md(prefix)}` |")
    lines.append("")
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
            url = _next_link(headers.get("Link", ""))
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
        status, _headers, _payload = self._request(
            "PATCH",
            f"https://api.github.com/repos/{self._repository}/issues/comments/{comment_id}",
            {"body": body},
            etag=etag,
        )
        if status == 412:
            raise Conflict(str(comment_id))
        if status != 200:
            raise RuntimeError(f"update comment {comment_id} failed: HTTP {status}")

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
    """Write one comment. On ETag conflict, re-read every marker comment and merge."""
    body = ""
    for _attempt in range(RETRY_LIMIT):
        matches = store.list_marker(header)
        if not matches:
            body = render_comment(state, now)
            store.create(body)
            matches = store.list_marker(header)
            if len(matches) <= 1:
                return body
        canonical = min(matches, key=lambda item: item.id)
        fresh = store.get(canonical.id)
        parsed = [parse_state(item.body) for item in matches]
        parsed.append(state)
        merged = merge_states([item for item in parsed if item])
        body = render_comment(merged, now)
        if fresh.body == body:
            _delete_extras(store, matches, fresh.id)
            return body
        try:
            store.update(fresh.id, body, fresh.etag)
        except Conflict:
            time.sleep(1)
            continue
        _delete_extras(store, matches, fresh.id)
        return body
    raise RuntimeError("shard progress comment kept conflicting; retries exhausted")


def _delete_extras(store: CommentStore, matches: list[CommentRecord], keep_id: int) -> None:
    for item in matches:
        if item.id != keep_id:
            try:
                store.delete(item.id)
            except RuntimeError as exc:
                print(f"warning: {exc}", file=sys.stderr)


def _load_failed_tests(path: str) -> list[str]:
    if not path or not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as handle:
        report = json.load(handle)
    if not isinstance(report, dict):
        return []
    return failed_test_names(report)


def _event_state(args: argparse.Namespace) -> dict[str, Any]:
    base = empty_state(args.run_id, args.preset, args.target, args.shard_count)
    base["run_url"] = args.run_url
    return apply_shard(
        base,
        shard_id=args.shard_id,
        result=args.result,
        started_at=args.started_at,
        finished_at=args.finished_at,
        job_url=args.job_url,
        log_prefix=args.log_prefix or f"shard_{args.shard_id}",
        failed_tests=_load_failed_tests(args.report),
        run_url=args.run_url,
    )


def _write_summary(path: str, body: str) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(body)


def _cmd_publish(args: argparse.Namespace) -> int:
    state = _event_state(args)
    if not args.pr:
        body = render_comment(state, args.finished_at)
        _write_summary(args.summary_file, body)
        print("No PR number; wrote the shard summary only.", file=sys.stderr)
        return 0
    token = os.environ.get("GITHUB_TOKEN", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repository:
        print("GITHUB_TOKEN or GITHUB_REPOSITORY is empty; skipping the PR comment.", file=sys.stderr)
        return 1
    store = GithubCommentStore(token, repository, int(args.pr))
    header = marker(args.run_id, args.preset)
    written = sync_comment(store, header, state, args.finished_at)
    _write_summary(args.summary_file, written)
    sys.stdout.write(written)
    return 0


def _cmd_finalize(args: argparse.Namespace) -> int:
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
    if not matches:
        print("No shard progress comment to finalize.", file=sys.stderr)
        return 0
    parsed = [parse_state(item.body) for item in matches]
    states = [item for item in parsed if item]
    if not states:
        print("Shard progress comments have no state.", file=sys.stderr)
        return 1
    merged = merge_states(states)
    now = args.now or format_time(datetime.now(timezone.utc))
    # Reuse the sync path so a racing shard is merged instead of overwritten.
    written = sync_comment(store, header, merged, now)
    _write_summary(args.summary_file, written)
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
    publish.add_argument("--summary-file", default="")
    publish.set_defaults(func=_cmd_publish)

    finalize = sub.add_parser("finalize", help="Merge marker comments into one final summary.")
    finalize.add_argument("--pr", default="")
    finalize.add_argument("--run-id", required=True)
    finalize.add_argument("--preset", required=True)
    finalize.add_argument("--now", default="")
    finalize.add_argument("--summary-file", default="")
    finalize.set_defaults(func=_cmd_finalize)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
