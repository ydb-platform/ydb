#!/usr/bin/env python3
"""Resolve a complete Codecov baseline that is an ancestor of the measured commit."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from codecov_suites import SUITES, suites_from_paths


API_ORIGIN = "https://api.codecov.io"
PAGE_SIZE = 100
MAX_PAGES = 100
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
FULL_CHECKPOINT_INTERVAL = 20
SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class ResolverError(RuntimeError):
    pass


def _request_json(
    url: str,
    headers: dict[str, str],
    attempts: int,
) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=headers)
    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                data = response.read(MAX_RESPONSE_BYTES + 1)
            if len(data) > MAX_RESPONSE_BYTES:
                raise ResolverError(f"API response is too large: {url}")
            payload = json.loads(data)
            if not isinstance(payload, dict):
                raise ResolverError(f"API returned a non-object JSON response: {url}")
            return payload
        except urllib.error.HTTPError as error:
            last_error = error
            if error.code != 429 and error.code < 500:
                raise ResolverError(f"API request failed with HTTP {error.code}: {url}") from error
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = error
        except json.JSONDecodeError as error:
            raise ResolverError(f"API returned invalid JSON: {url}") from error

        if attempt + 1 < attempts:
            time.sleep(2**attempt)

    raise ResolverError(f"API request failed after {attempts} attempts: {url}") from last_error


def _validate_sha(value: str, label: str) -> str:
    if not SHA_RE.fullmatch(value):
        raise ResolverError(f"{label} must contain exactly 40 hexadecimal characters")
    return value.lower()


def _run_git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", *args],
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or error.stdout.strip() or f"exit code {error.returncode}"
        raise ResolverError(f"git {' '.join(args)} failed: {detail}") from error


def _require_local_commit(sha: str, label: str) -> None:
    try:
        _run_git("rev-parse", "--verify", f"{sha}^{{commit}}")
    except ResolverError as error:
        raise ResolverError(f"{label} {sha} is unavailable in the local checkout") from error


def _is_ancestor(candidate: str, descendant: str) -> bool:
    result = _run_git("merge-base", "--is-ancestor", candidate, descendant, check=False)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    raise ResolverError(
        f"git merge-base --is-ancestor {candidate} {descendant} failed: {detail}"
    )


class CodecovApi:
    def __init__(
        self,
        repository: str,
        attempts: int = 3,
    ) -> None:
        if not REPOSITORY_RE.fullmatch(repository):
            raise ResolverError("repository must have the form owner/name")
        self.owner, self.repo = repository.split("/", 1)
        self.attempts = attempts

    def _request(self, path: str, params: dict[str, str | int]) -> dict[str, Any]:
        query = urllib.parse.urlencode(params)
        url = f"{API_ORIGIN}{path}?{query}"
        return _request_json(
            url,
            {"Accept": "application/json", "User-Agent": "ydb-codecov-parent-resolver"},
            self.attempts,
        )

    def _paginated_results(self, path: str, params: dict[str, str | int]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._request(path, {**params, "page": page, "page_size": PAGE_SIZE})
            page_results = payload.get("results")
            next_page = payload.get("next")
            if not isinstance(page_results, list):
                raise ResolverError("Codecov pagination response has an unexpected schema")
            if next_page is not None and (not isinstance(next_page, str) or not next_page):
                raise ResolverError("Codecov pagination response has an invalid next page")
            for item in page_results:
                if not isinstance(item, dict):
                    raise ResolverError("Codecov result item has an unexpected schema")
                results.append(item)
            if next_page is None:
                return results
            if page >= MAX_PAGES:
                raise ResolverError(f"Codecov pagination exceeds {MAX_PAGES} pages")
            # Do not follow the server-provided URL. Keep the request on the
            # validated Codecov origin/path and advance only the page number.
            page += 1

    def branch_commits(self, branch: str) -> list[dict[str, Any]]:
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        return self._paginated_results(
            f"/api/v2/github/{owner}/repos/{repo}/commits/",
            {"branch": branch},
        )

    def commit_flag_sets(self, sha: str) -> tuple[set[str], set[str]]:
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        uploads = self._paginated_results(
            f"/api/v2/github/{owner}/repos/{repo}/commits/{sha}/uploads/",
            {},
        )
        flags: set[str] = set()
        actual_flags: set[str] = set()
        for upload in uploads:
            state = str(upload.get("state", "")).lower()
            state_name = str(upload.get("state_name", "")).upper()
            totals = upload.get("totals")
            upload_flags = upload.get("flags")
            if state not in {"complete", "merged"} and state_name != "MERGED":
                continue
            if not isinstance(totals, dict) or not isinstance(totals.get("files"), int):
                continue
            if totals["files"] <= 0 or not isinstance(upload_flags, list):
                continue
            valid_flags = {flag for flag in upload_flags if isinstance(flag, str)}
            flags.update(valid_flags)
            if upload.get("upload_type") == "uploaded":
                actual_flags.update(valid_flags)
        return flags, actual_flags


def _expected_actual_flags(baseline: str, commit: str, required_flags: set[str]) -> set[str]:
    baseline = _validate_sha(baseline, "trusted baseline SHA")
    _require_local_commit(baseline, "trusted baseline SHA")
    if not _is_ancestor(baseline, commit):
        raise ResolverError(f"Trusted baseline {baseline} is not an ancestor of {commit}")
    changed = _run_git(
        "-c",
        "core.quotePath=false",
        "log",
        "--first-parent",
        "--diff-merges=first-parent",
        "--format=",
        "--name-only",
        "--no-renames",
        f"{baseline}..{commit}",
        "--",
    ).stdout.splitlines()
    suites = suites_from_paths(changed)
    return {str(SUITES[suite]["flag"]) for suite in suites}


def _resolve_parent(
    api: CodecovApi,
    branch: str,
    main_anchor: str,
    report_head: str,
    required_flags: set[str],
) -> tuple[str, bool]:
    main_anchor = _validate_sha(main_anchor, "main anchor SHA")
    report_head = _validate_sha(report_head, "report head SHA")
    _require_local_commit(main_anchor, "main anchor SHA")
    _require_local_commit(report_head, "report head SHA")

    history = _run_git("rev-list", "--first-parent", main_anchor).stdout.splitlines()
    ranks = {sha.lower(): rank for rank, sha in enumerate(history) if SHA_RE.fullmatch(sha)}
    if main_anchor not in ranks:
        raise ResolverError("main anchor is missing from its first-parent history")

    candidates: dict[str, int] = {}
    for commit in api.branch_commits(branch):
        sha = commit.get("commitid")
        if not isinstance(sha, str) or not SHA_RE.fullmatch(sha):
            raise ResolverError("Codecov returned an invalid commit SHA")
        sha = sha.lower()
        totals = commit.get("totals")
        if commit.get("state") != "complete" or not isinstance(totals, dict):
            continue
        if not isinstance(totals.get("sessions"), int) or totals["sessions"] <= 0:
            continue
        if sha not in ranks or not _is_ancestor(sha, report_head):
            continue
        candidates[sha] = ranks[sha]

    ordered = sorted(candidates.items(), key=lambda item: item[1], reverse=True)
    reports: dict[str, tuple[set[str], set[str]]] = {}

    # Any report with a fresh upload for every required flag is an independent
    # trusted checkpoint. Search newest-first so normal runs need only one
    # uploads API request even as main accumulates coverage history.
    seed_index: int | None = None
    for index in range(len(ordered) - 1, -1, -1):
        sha = ordered[index][0]
        reports[sha] = api.commit_flag_sets(sha)
        flags, actual_flags = reports[sha]
        if required_flags <= flags and required_flags <= actual_flags:
            seed_index = index
            break

    if seed_index is None:
        raise ResolverError(
            "No complete Codecov main baseline is an ancestor of the measured commit. "
            "Rebase the PR after a full main coverage run."
        )

    trusted_sha = ordered[seed_index][0]
    trusted_flags = reports[trusted_sha][0]
    print(
        f"Validated full Codecov seed {trusted_sha} with flags: "
        f"{', '.join(sorted(trusted_flags))}",
        file=sys.stderr,
    )

    # Validate newer reports from oldest to newest. Do not trust Codecov's
    # inferred parent: an asynchronously processed partial run may point at
    # another partial run. Every incremental report must contain fresh uploads
    # for all suites changed since the last baseline that we proved complete.
    for sha, _ in ordered[seed_index + 1 :]:
        flags, actual_flags = reports[sha]
        missing = sorted(required_flags - flags)
        if missing:
            print(
                f"Skipping incomplete Codecov baseline {sha}; missing flags: {', '.join(missing)}",
                file=sys.stderr,
            )
            continue

        expected_actual = _expected_actual_flags(trusted_sha, sha, required_flags)
        missing_actual = sorted(expected_actual - actual_flags)
        if not expected_actual:
            print(
                f"Skipping Codecov baseline {sha}; no coverage suite was touched",
                file=sys.stderr,
            )
            continue

        if missing_actual:
            print(
                f"Skipping Codecov baseline {sha}; flags without a fresh upload: "
                f"{', '.join(missing_actual)}",
                file=sys.stderr,
            )
            continue

        trusted_sha = sha
        trusted_flags = flags
        print(
            f"Validated complete Codecov baseline {sha} with flags: "
            f"{', '.join(sorted(flags))}",
            file=sys.stderr,
        )

    print(
        f"Using complete Codecov baseline {trusted_sha} with flags: "
        f"{', '.join(sorted(trusted_flags))}",
        file=sys.stderr,
    )
    incremental_reports = len(ordered) - seed_index - 1
    force_full = incremental_reports >= FULL_CHECKPOINT_INTERVAL
    if force_full:
        print(
            f"Forcing a full checkpoint after {incremental_reports} incremental reports",
            file=sys.stderr,
        )
    return trusted_sha, force_full


def resolve_parent_sha(
    api: CodecovApi,
    branch: str,
    main_anchor: str,
    report_head: str,
    required_flags: set[str],
) -> str:
    return _resolve_parent(api, branch, main_anchor, report_head, required_flags)[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True, help="GitHub repository as owner/name")
    parser.add_argument("--branch", default="main", help="Codecov baseline branch")
    parser.add_argument("--main-anchor", required=True, help="Main SHA whose first-parent history is searched")
    parser.add_argument("--report-head", required=True, help="Measured SHA that must descend from the baseline")
    parser.add_argument(
        "--output-format",
        choices=("sha", "github"),
        default="sha",
        help="Print either the SHA or GitHub Actions output assignments",
    )
    args = parser.parse_args()

    try:
        parent, force_full = _resolve_parent(
            CodecovApi(args.repository),
            args.branch,
            args.main_anchor,
            args.report_head,
            {str(config["flag"]) for config in SUITES.values()},
        )
    except ResolverError as error:
        print(f"Failed to resolve Codecov parent: {error}", file=sys.stderr)
        return 1

    if args.output_format == "github":
        print(f"parent_sha={parent}")
        print(f"force_full={'true' if force_full else 'false'}")
    else:
        print(parent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
