#!/usr/bin/env python3
"""Mark PR-check results stale when checks are older than 24h or base branch moved."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

MAX_CHECK_AGE = dt.timedelta(hours=24)
WORKFLOW_FILE = "pr_check.yml"
INTEGRATED_CONTEXT = "checks_integrated"
REQUIRED_STATUS_PREFIXES = (
    "build_relwithdebinfo",
    "build_release-asan",
    "test_relwithdebinfo",
    "test_release-asan",
)
COMMENT_HEADER = "<!-- stale-pr-check -->\n"
MERGE_SHA_RE = re.compile(r"merge:([0-9a-f]{40})", re.IGNORECASE)
CHECKED_AT_RE = re.compile(r"at:([^|]+)", re.IGNORECASE)


def github_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    return session


def parse_github_time(value: str) -> dt.datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value).astimezone(dt.timezone.utc)


def get_open_pulls(
    session: requests.Session,
    owner: str,
    repo: str,
    base_branch: Optional[str] = None,
) -> List[Dict[str, Any]]:
    pulls: List[Dict[str, Any]] = []
    page = 1
    while True:
        params: Dict[str, Any] = {
            "state": "open",
            "per_page": 100,
            "page": page,
        }
        if base_branch:
            params["base"] = base_branch
        response = session.get(
            f"https://api.github.com/repos/{owner}/{repo}/pulls",
            params=params,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        pulls.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return pulls


def get_commit_statuses(
    session: requests.Session,
    owner: str,
    repo: str,
    sha: str,
) -> List[Dict[str, Any]]:
    response = session.get(
        f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}/status"
    )
    response.raise_for_status()
    return response.json().get("statuses", [])


def latest_status_by_context(
    statuses: List[Dict[str, Any]],
    context: str,
) -> Optional[Dict[str, Any]]:
    matched = [status for status in statuses if status.get("context") == context]
    if not matched:
        return None
    return max(matched, key=lambda status: parse_github_time(status["created_at"]))


def required_build_test_statuses(statuses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for status in statuses:
        context = status.get("context", "")
        if not any(context.startswith(prefix) for prefix in REQUIRED_STATUS_PREFIXES):
            continue
        previous = latest.get(context)
        if previous is None or parse_github_time(status["created_at"]) > parse_github_time(
            previous["created_at"]
        ):
            latest[context] = status
    return list(latest.values())


def has_green_required_checks(statuses: List[Dict[str, Any]]) -> bool:
    required = required_build_test_statuses(statuses)
    if len(required) != 4:
        return False
    return all(status.get("state") == "success" for status in required)


def parse_validated_merge(description: str) -> Tuple[Optional[str], Optional[dt.datetime]]:
    merge_match = MERGE_SHA_RE.search(description or "")
    checked_at_match = CHECKED_AT_RE.search(description or "")
    merge_sha = merge_match.group(1).lower() if merge_match else None
    checked_at = None
    if checked_at_match:
        raw = checked_at_match.group(1).strip()
        try:
            checked_at = parse_github_time(raw)
        except ValueError:
            checked_at = None
    return merge_sha, checked_at


def get_last_successful_pr_check_run(
    session: requests.Session,
    owner: str,
    repo: str,
    head_sha: str,
) -> Optional[Dict[str, Any]]:
    response = session.get(
        f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{WORKFLOW_FILE}/runs",
        params={
            "event": "pull_request_target",
            "head_sha": head_sha,
            "status": "success",
            "per_page": 5,
        },
    )
    response.raise_for_status()
    runs = response.json().get("workflow_runs", [])
    if not runs:
        return None
    return max(runs, key=lambda run: parse_github_time(run["updated_at"]))


def evaluate_staleness(
    *,
    now: dt.datetime,
    current_merge_sha: Optional[str],
    integrated_status: Optional[Dict[str, Any]],
    last_successful_run: Optional[Dict[str, Any]],
) -> Tuple[bool, str]:
    validated_merge_sha = None
    checked_at = None
    if integrated_status and integrated_status.get("state") == "success":
        validated_merge_sha, checked_at = parse_validated_merge(
            integrated_status.get("description", "")
        )

    if checked_at is not None:
        age = now - checked_at
        if age > MAX_CHECK_AGE:
            hours = int(age.total_seconds() // 3600)
            return True, (
                f"last successful PR-check is older than 24 hours "
                f"({hours}h, checked at {checked_at.strftime('%Y-%m-%d %H:%M UTC')})"
            )

    if (
        validated_merge_sha
        and current_merge_sha
        and validated_merge_sha != current_merge_sha.lower()
    ):
        return True, (
            "base branch moved since the last PR-check "
            f"(tested merge `{validated_merge_sha[:12]}`, "
            f"current merge `{current_merge_sha[:12]}`)"
        )

    if validated_merge_sha is None and last_successful_run is not None:
        run_time = parse_github_time(last_successful_run["updated_at"])
        age = now - run_time
        if age > MAX_CHECK_AGE:
            hours = int(age.total_seconds() // 3600)
            return True, (
                f"last successful PR-check workflow run is older than 24 hours "
                f"({hours}h, run updated at {run_time.strftime('%Y-%m-%d %H:%M UTC')})"
            )

    return False, ""


def find_stale_comment(
    session: requests.Session,
    owner: str,
    repo: str,
    issue_number: int,
) -> Optional[Dict[str, Any]]:
    page = 1
    while True:
        response = session.get(
            f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments",
            params={"per_page": 100, "page": page},
        )
        response.raise_for_status()
        comments = response.json()
        if not comments:
            return None
        for comment in comments:
            body = comment.get("body") or ""
            if body.startswith(COMMENT_HEADER):
                return comment
        if len(comments) < 100:
            return None
        page += 1


def build_comment(reason: str) -> str:
    return (
        f"{COMMENT_HEADER}"
        ":red_circle: **PR-check results are stale and cannot be used for merge.**\n\n"
        f"{reason}\n\n"
        "Please re-run checks by pushing a commit or adding the `rebase-and-check` label."
    )


def post_commit_status(
    session: requests.Session,
    owner: str,
    repo: str,
    sha: str,
    *,
    state: str,
    description: str,
    target_url: str,
) -> None:
    response = session.post(
        f"https://api.github.com/repos/{owner}/{repo}/statuses/{sha}",
        json={
            "state": state,
            "description": description[:140],
            "context": INTEGRATED_CONTEXT,
            "target_url": target_url,
        },
    )
    response.raise_for_status()


def process_pull(
    session: requests.Session,
    owner: str,
    repo: str,
    pull: Dict[str, Any],
    *,
    dry_run: bool,
    run_url: str,
) -> bool:
    if pull.get("draft"):
        return False

    head_sha = pull["head"]["sha"]
    statuses = get_commit_statuses(session, owner, repo, head_sha)
    integrated_status = latest_status_by_context(statuses, INTEGRATED_CONTEXT)
    if integrated_status is None or integrated_status.get("state") != "success":
        return False
    if not has_green_required_checks(statuses):
        return False

    pull_number = pull["number"]
    fresh_pull = session.get(
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}"
    )
    fresh_pull.raise_for_status()
    pull_data = fresh_pull.json()
    current_merge_sha = pull_data.get("merge_commit_sha")
    last_run = get_last_successful_pr_check_run(session, owner, repo, head_sha)
    now = dt.datetime.now(dt.timezone.utc)
    is_stale, reason = evaluate_staleness(
        now=now,
        current_merge_sha=current_merge_sha,
        integrated_status=integrated_status,
        last_successful_run=last_run,
    )
    if not is_stale:
        existing_comment = find_stale_comment(session, owner, repo, pull_number)
        if existing_comment and not dry_run:
            response = session.delete(
                f"https://api.github.com/repos/{owner}/{repo}/issues/comments/{existing_comment['id']}"
            )
            response.raise_for_status()
        return False

    print(f"PR #{pull_number} is stale: {reason}")
    body = build_comment(reason)
    existing_comment = find_stale_comment(session, owner, repo, pull_number)
    if dry_run:
        print(body)
        return True

    if existing_comment:
        response = session.patch(
            f"https://api.github.com/repos/{owner}/{repo}/issues/comments/{existing_comment['id']}",
            json={"body": body},
        )
        response.raise_for_status()
    else:
        response = session.post(
            f"https://api.github.com/repos/{owner}/{repo}/issues/{pull_number}/comments",
            json={"body": body},
        )
        response.raise_for_status()

    post_commit_status(
        session,
        owner,
        repo,
        head_sha,
        state="failure",
        description="PR-check stale: re-run required",
        target_url=run_url,
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", default=os.environ.get("GITHUB_REPOSITORY_OWNER"))
    parser.add_argument("--repo", default=None)
    parser.add_argument("--base-branch", default=None)
    parser.add_argument("--pull-number", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("GITHUB_TOKEN is required", file=sys.stderr)
        return 1

    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if args.repo is None and repository:
        owner, repo = repository.split("/", 1)
    else:
        owner = args.owner
        repo = args.repo
    if not owner or not repo:
        print("Repository owner/repo is required", file=sys.stderr)
        return 1

    run_url = os.environ.get(
        "GITHUB_RUN_URL",
        f"https://github.com/{owner}/{repo}/actions",
    )
    session = github_session(token)

    if args.pull_number is not None:
        response = session.get(
            f"https://api.github.com/repos/{owner}/{repo}/pulls/{args.pull_number}"
        )
        response.raise_for_status()
        pulls = [response.json()]
    else:
        pulls = get_open_pulls(session, owner, repo, args.base_branch)

    stale_count = 0
    for pull in pulls:
        if pull.get("state") != "open":
            continue
        if process_pull(
            session,
            owner,
            repo,
            pull,
            dry_run=args.dry_run,
            run_url=run_url,
        ):
            stale_count += 1

    print(f"Marked stale PRs: {stale_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
