#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request


class ScriptError(RuntimeError):
    pass


RETRY_ATTEMPTS = 4
RETRY_BASE_DELAY_SEC = 2
TRANSIENT_NETWORK_MARKERS = (
    "timed out",
    "timeout",
    "connection reset",
    "connection was reset",
    "could not resolve host",
    "temporary failure in name resolution",
    "network is unreachable",
    "connection refused",
    "failed to connect",
    "the remote end hung up unexpectedly",
    "rpc failed",
    "http 502",
    "http 503",
    "http 504",
    "bad gateway",
    "service unavailable",
    "internal server error",
)
VERBOSE = False


def log(message: str) -> None:
    if VERBOSE:
        print(f"[prepare-merge] {message}")


def run(
    cmd: list[str],
    *,
    capture_output: bool = True,
    check: bool = True,
    text: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=capture_output,
        check=check,
        text=text,
    )


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(["git", *args], check=check)


def is_transient_network_error(output: str) -> bool:
    text = output.lower()
    return any(marker in text for marker in TRANSIENT_NETWORK_MARKERS)


def run_with_retry(cmd: list[str], operation_name: str) -> subprocess.CompletedProcess[str]:
    last_result: subprocess.CompletedProcess[str] | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        log(f"{operation_name}: attempt {attempt}/{RETRY_ATTEMPTS}")
        result = run(cmd, check=False)
        if result.returncode == 0:
            log(f"{operation_name}: success")
            return result

        last_result = result
        merged_output = f"{result.stdout}\n{result.stderr}".strip()
        should_retry = attempt < RETRY_ATTEMPTS and is_transient_network_error(merged_output)
        if should_retry:
            log(f"{operation_name}: transient error, retrying after {RETRY_BASE_DELAY_SEC * attempt}s")
            time.sleep(RETRY_BASE_DELAY_SEC * attempt)
            continue

        break

    assert last_result is not None
    merged_output = f"{last_result.stdout}\n{last_result.stderr}".strip()
    raise ScriptError(
        f"{operation_name} failed after {RETRY_ATTEMPTS} attempts (exit={last_result.returncode}): {merged_output}"
    )


def git_fetch_with_retry(*args: str) -> subprocess.CompletedProcess[str]:
    return run_with_retry(["git", "fetch", *args], f"git fetch {' '.join(args)}")


def git_ls_remote_with_retry(*args: str) -> subprocess.CompletedProcess[str]:
    return run_with_retry(["git", "ls-remote", *args], f"git ls-remote {' '.join(args)}")


def git_push_with_retry(*args: str) -> subprocess.CompletedProcess[str]:
    return run_with_retry(["git", "push", *args], f"git push {' '.join(args)}")


def get_github_token() -> str | None:
    return os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")


def fetch_pr_json_with_retry(repo: str, pr_number: int) -> dict[str, Any]:
    token = get_github_token()
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        log(f"GET {url}: attempt {attempt}/{RETRY_ATTEMPTS}")
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        req = request.Request(url, headers=headers, method="GET")
        try:
            with request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
            log(f"GET {url}: success")
            return json.loads(body)
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            is_transient = exc.code in {429, 500, 502, 503, 504}
            if attempt < RETRY_ATTEMPTS and is_transient:
                delay = RETRY_BASE_DELAY_SEC * attempt
                log(f"GET {url}: HTTP {exc.code}, retrying after {delay}s")
                time.sleep(delay)
                continue
            raise ScriptError(f"GitHub API request failed (HTTP {exc.code}): {body}") from exc
        except error.URLError as exc:
            is_transient = True
            if attempt < RETRY_ATTEMPTS and is_transient:
                delay = RETRY_BASE_DELAY_SEC * attempt
                log(f"GET {url}: network error ({exc.reason}), retrying after {delay}s")
                time.sleep(delay)
                continue
            raise ScriptError(f"GitHub API network error: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise ScriptError(f"failed to decode GitHub API response: {exc}") from exc

    raise ScriptError("unexpected error while fetching PR metadata")


def require_tool(tool: str) -> None:
    result = run(["bash", "-lc", f"command -v {tool}"], check=False)
    if result.returncode != 0:
        raise ScriptError(f"required command not found: {tool}")


def detect_repo_from_origin() -> str:
    remote = git("remote", "get-url", "origin", check=False)
    if remote.returncode != 0:
        raise ScriptError("cannot detect origin remote URL; pass --repo owner/repo")

    url = remote.stdout.strip()
    if not url:
        raise ScriptError("origin remote URL is empty; pass --repo owner/repo")

    patterns = [
        r"^git@github\.com:(?P<repo>.+?)(?:\.git)?$",
        r"^https://github\.com/(?P<repo>.+?)(?:\.git)?$",
        r"^ssh://git@github\.com/(?P<repo>.+?)(?:\.git)?$",
    ]
    for pattern in patterns:
        match = re.match(pattern, url)
        if match:
            return match.group("repo")

    raise ScriptError(f"unsupported origin URL format: {url}")


def ensure_clean_worktree() -> None:
    status = git("status", "--porcelain")
    if status.stdout.strip():
        raise ScriptError("working tree is not clean; commit/stash changes first")


def current_ref() -> str:
    branch = git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    if branch == "HEAD":
        return git("rev-parse", "HEAD").stdout.strip()
    return branch


def load_pr(repo: str, pr_number: int) -> dict[str, Any]:
    log(f"Loading PR metadata for {repo}#{pr_number}")
    return fetch_pr_json_with_retry(repo, pr_number)


@dataclass
class MergeContext:
    repo: str
    pr_number: int
    base_ref: str
    base_repo: str
    base_sha_api: str
    base_sha_local: str
    head_ref: str
    head_repo: str
    head_sha_api: str
    head_sha_local: str
    target_ref: str


def parse_ls_remote_ref(output: str, target_ref: str) -> str | None:
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[1] == target_ref:
            return parts[0]
    return None


def get_commit_parents(commit_sha: str) -> list[str]:
    parents = git("show", "-s", "--format=%P", commit_sha).stdout.strip().split()
    return parents


def is_ancestor(ancestor_sha: str, descendant_sha: str) -> bool:
    check = git("merge-base", "--is-ancestor", ancestor_sha, descendant_sha, check=False)
    if check.returncode == 0:
        return True
    if check.returncode == 1:
        return False
    raise ScriptError(
        f"git merge-base --is-ancestor failed for {ancestor_sha} -> {descendant_sha} (exit={check.returncode})"
    )


def get_commit_created_at_utc(commit_sha: str) -> str:
    raw_iso = git("show", "-s", "--format=%cI", commit_sha).stdout.strip()
    dt = datetime.fromisoformat(raw_iso)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_commit_created_at_epoch(commit_sha: str) -> int:
    return int(git("show", "-s", "--format=%ct", commit_sha).stdout.strip())


def inspect_existing_merge_ref(ctx: MergeContext) -> tuple[str | None, bool]:
    log(f"Inspecting existing merge ref: {ctx.target_ref}")
    ls_remote = git_ls_remote_with_retry("--refs", "origin", ctx.target_ref)
    remote_sha = parse_ls_remote_ref(ls_remote.stdout, ctx.target_ref)
    if remote_sha is None:
        log("No existing merge ref found")
        return None, False

    git_fetch_with_retry("origin", ctx.target_ref)
    fetched_sha = git("rev-parse", "FETCH_HEAD").stdout.strip()
    if remote_sha != fetched_sha:
        log(
            f"Merge ref moved between ls-remote and fetch: {remote_sha} -> {fetched_sha}"
        )
    parents = get_commit_parents(fetched_sha)
    if len(parents) != 2:
        # Special case: PR head is already fully contained in base.
        # In this state we can legitimately reuse base tip SHA (single-parent commit)
        # as an effective merge ref for test runs.
        if fetched_sha == ctx.base_sha_local and is_ancestor(ctx.head_sha_local, ctx.base_sha_local):
            log(
                "Existing merge ref points to base tip and PR head is already contained in base; "
                f"treating as current: {fetched_sha}"
            )
            return fetched_sha, True
        log(f"Existing merge ref is not a merge commit: {fetched_sha}")
        return fetched_sha, False

    is_current = parents[0] == ctx.base_sha_local and parents[1] == ctx.head_sha_local
    log(
        f"Existing merge ref sha={fetched_sha}, parent1={parents[0]}, parent2={parents[1]}, "
        f"is_current={is_current}"
    )
    return fetched_sha, is_current


def _short_git_sha(sha: str, length: int = 7) -> str:
    if len(sha) <= length:
        return sha
    return sha[:length]


def merge_commit_message_subject(ctx: MergeContext) -> str:
    """Single-line subject: branches + SHAs (GitHub-style, more context than default merge)."""
    head_s = _short_git_sha(ctx.head_sha_local)
    base_s = _short_git_sha(ctx.base_sha_local)
    if ctx.head_repo == ctx.repo:
        return (
            f"Merge PR #{ctx.pr_number}: '{ctx.head_ref}' ({head_s}) into "
            f"'{ctx.base_ref}' ({base_s})"
        )
    return (
        f"Merge PR #{ctx.pr_number}: {ctx.head_repo} '{ctx.head_ref}' ({head_s}) into "
        f"'{ctx.base_ref}' ({base_s})"
    )


def merge_commit_message_body(ctx: MergeContext) -> str:
    return (
        f"CI merge ref for https://github.com/{ctx.repo}/pull/{ctx.pr_number}\n"
        f"head {ctx.head_sha_local}\n"
        f"base {ctx.base_sha_local}"
    )


def build_merge_commit(
    *,
    ctx: MergeContext,
    merge_input: str,
    keep_branch: bool,
    origin_ref: str,
) -> tuple[str | None, str, str | None]:
    merge_branch = f"pr-{ctx.pr_number}-merge-{int(time.time())}"
    log(f"Creating local merge branch: {merge_branch} from origin/{ctx.base_ref}")
    git("checkout", "-B", merge_branch, f"origin/{ctx.base_ref}")

    merge_subject = merge_commit_message_subject(ctx)
    merge_body = merge_commit_message_body(ctx)
    merge_cmd = [
        "-c",
        "user.name=github-actions[bot]",
        "-c",
        "user.email=github-actions[bot]@users.noreply.github.com",
        "merge",
        "--no-ff",
        "-m",
        merge_subject,
        "-m",
        merge_body,
        merge_input,
    ]
    log(f"Running merge with input: {merge_input}")
    merge_process = git(*merge_cmd, check=False)
    if merge_process.returncode != 0:
        # Best-effort cleanup to avoid leaving the repo in conflicted state.
        log("Merge failed, attempting cleanup")
        git("merge", "--abort", check=False)
        if not keep_branch:
            git("checkout", origin_ref, check=False)
            git("branch", "-D", merge_branch, check=False)
        return (
            None,
            "dirty",
            merge_process.stdout.strip() or merge_process.stderr.strip() or "merge failed",
        )

    merge_output = f"{merge_process.stdout}\n{merge_process.stderr}".strip().lower()
    merge_sha = git("rev-parse", "HEAD").stdout.strip()
    log(f"Created merge commit: {merge_sha}")
    parents = get_commit_parents(merge_sha)
    if len(parents) != 2:
        if (
            merge_sha == ctx.base_sha_local
            and ("already up to date" in merge_output or is_ancestor(ctx.head_sha_local, ctx.base_sha_local))
        ):
            log(
                "Merge reported 'Already up to date': PR head is already contained in base tip. "
                f"Using base tip SHA as effective merge commit: {merge_sha}"
            )
            if not keep_branch:
                git("checkout", origin_ref)
                git("branch", "-D", merge_branch)
            log(f"Merge branch cleanup completed, back on: {origin_ref}")
            return merge_sha, "clean", None
        raise ScriptError(f"created commit is not a merge commit: {merge_sha}")
    if parents[0] != ctx.base_sha_local:
        raise ScriptError(
            "unexpected parent order: first parent is not current base tip "
            f"(expected {ctx.base_sha_local}, got {parents[0]})"
        )
    if parents[1] != ctx.head_sha_local:
        raise ScriptError(
            "unexpected second parent: not the current head SHA "
            f"(expected {ctx.head_sha_local}, got {parents[1]})"
        )

    if not keep_branch:
        git("checkout", origin_ref)
        git("branch", "-D", merge_branch)
    log(f"Merge branch cleanup completed, back on: {origin_ref}")
    return merge_sha, "clean", None


def write_json_output(path: str | None, payload: dict[str, Any]) -> None:
    content = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    if path:
        Path(path).write_text(content + "\n", encoding="utf-8")
    else:
        print(content)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create/update deterministic PR merge commit ref and return api-like JSON."
        )
    )
    parser.add_argument("pr_number", type=int, help="Pull request number")
    parser.add_argument("--repo", help="owner/repo (default: detect from origin remote)")
    parser.add_argument("--base-ref", help="override PR base ref")
    parser.add_argument(
        "--get-info",
        action="store_true",
        help="only read info from existing merge ref; do not create or push",
    )

    push_group = parser.add_mutually_exclusive_group()
    push_group.add_argument(
        "--push",
        dest="push",
        action="store_true",
        help="push merge ref to origin (default)",
    )
    push_group.add_argument(
        "--only-local",
        dest="push",
        action="store_false",
        help="do not push merge ref to origin",
    )
    parser.set_defaults(push=True)

    parser.add_argument(
        "--ref-prefix",
        default="refs/pr-ci",
        help="prefix for pushed ref (default: refs/pr-ci)",
    )
    parser.add_argument(
        "--keep-branch",
        action="store_true",
        help="keep local temporary merge branch checked out",
    )
    parser.add_argument(
        "--no-reuse-if-unchanged",
        action="store_true",
        help="always recreate merge commit even if existing ref matches base/head",
    )
    parser.add_argument(
        "--json-out",
        help="write api-like JSON result to file (default: print to stdout)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print detailed progress logs to stdout",
    )
    args = parser.parse_args()
    global VERBOSE
    VERBOSE = args.verbose

    require_tool("git")

    repo = args.repo or detect_repo_from_origin()
    log(f"Resolved repository: {repo}")
    if not args.get_info:
        log("Checking that working tree is clean")
        ensure_clean_worktree()
    origin_ref = current_ref()
    log(f"Current ref: {origin_ref}")

    pr = load_pr(repo, args.pr_number)
    base_ref = args.base_ref or pr["base"]["ref"]
    base_repo = pr["base"]["repo"]["full_name"]
    base_sha_api = pr["base"]["sha"]
    head_ref = pr["head"]["ref"]
    head_repo = pr["head"]["repo"]["full_name"]
    head_sha_api = pr["head"]["sha"]
    target_ref = f"{args.ref_prefix}/{args.pr_number}/merge"
    log(f"Target merge ref: {target_ref}")

    git_fetch_with_retry("origin", base_ref)
    base_sha_local = git("rev-parse", f"origin/{base_ref}").stdout.strip()
    log(f"Base sha current={base_sha_local}, pr_snapshot={base_sha_api}")

    if head_repo == repo:
        log("Head repository matches base repository, fetching by head SHA from origin")
        git_fetch_with_retry("origin", head_sha_api)
        merge_input = head_sha_api
        head_sha_local = head_sha_api
    else:
        pr_head_ref = f"refs/pull/{args.pr_number}/head"
        log(f"Head from fork {head_repo}, fetching {pr_head_ref} from origin")
        git_fetch_with_retry("origin", pr_head_ref)
        head_sha_local = git("rev-parse", "FETCH_HEAD").stdout.strip()
        if head_sha_local != head_sha_api:
            raise ScriptError(
                "PR head changed while preparing merge ref: "
                f"fetched {head_sha_local}, expected {head_sha_api}"
            )
        merge_input = head_sha_local
    log(f"Head sha current={head_sha_local}, pr_snapshot={head_sha_api}")

    ctx = MergeContext(
        repo=repo,
        pr_number=args.pr_number,
        base_ref=base_ref,
        base_repo=base_repo,
        base_sha_api=base_sha_api,
        base_sha_local=base_sha_local,
        head_ref=head_ref,
        head_repo=head_repo,
        head_sha_api=head_sha_api,
        head_sha_local=head_sha_local,
        target_ref=target_ref,
    )

    existing_ref_sha, existing_is_current = inspect_existing_merge_ref(ctx)
    merge_commit_update_required = not existing_is_current
    log(f"merge_commit_update_required={merge_commit_update_required}")

    merge_commit_sha: str | None = None
    mergeable = False
    mergeable_state = "unknown"
    error_message: str | None = None

    if existing_is_current and not args.no_reuse_if_unchanged:
        merge_commit_sha = existing_ref_sha
        mergeable = True
        mergeable_state = "clean"
        log(f"Reusing existing merge commit: {merge_commit_sha}")
    else:
        if not args.get_info:
            merge_commit_sha, mergeable_state, error_message = build_merge_commit(
                ctx=ctx,
                merge_input=merge_input,
                keep_branch=args.keep_branch,
                origin_ref=origin_ref,
            )
            mergeable = merge_commit_sha is not None
            log(
                f"Create-merge result: mergeable={mergeable}, "
                f"merge_state={mergeable_state}, merge_commit_sha={merge_commit_sha}"
            )
        else:
            mergeable = False
            mergeable_state = "unknown"
            log("Info-only mode and existing merge ref is stale/missing")

    if args.push and not args.get_info and mergeable and merge_commit_sha is not None:
        log(f"Pushing merge ref to origin:{ctx.target_ref}")
        git_push_with_retry("origin", f"{merge_commit_sha}:{ctx.target_ref}", "--force")
    elif args.push and not args.get_info and mergeable and merge_commit_sha is None:
        log("Push requested but merge_commit_sha is empty, skipping push")
    elif args.get_info:
        log("Info-only mode: push skipped")
    else:
        log("Only-local mode: push skipped")

    payload: dict[str, Any] = {
        "pr_number": args.pr_number,
        "pr_url": f"https://github.com/{repo}/pull/{args.pr_number}",
        "repository": repo,
        "pr_base_ref": base_ref,
        "pr_base_sha": base_sha_api,
        "pr_head_ref": head_ref,
        "pr_head_sha": head_sha_api,
        "merge_base_sha": base_sha_local,
        "merge_base_created_at": get_commit_created_at_utc(base_sha_local),
        "merge_base_created_at_epoch": get_commit_created_at_epoch(base_sha_local),
        "merge_head_sha": head_sha_local,
        "merge_head_created_at": get_commit_created_at_utc(head_sha_local),
        "merge_head_created_at_epoch": get_commit_created_at_epoch(head_sha_local),
        "mergeable": mergeable,
        "mergeable_state": mergeable_state,
        "merge_commit_sha": merge_commit_sha,
        "merge_commit_url": (
            f"https://github.com/{repo}/commit/{merge_commit_sha}" if merge_commit_sha is not None else None
        ),
        "merge_commit_created_at": (
            get_commit_created_at_utc(merge_commit_sha) if merge_commit_sha is not None else None
        ),
        "merge_commit_created_at_epoch": (
            get_commit_created_at_epoch(merge_commit_sha) if merge_commit_sha is not None else None
        ),
        "merge_ref": ctx.target_ref,
        "merge_ref_url": f"https://github.com/{repo}/tree/{ctx.target_ref}",
        "merge_commit_update_required": merge_commit_update_required,
        "error": error_message,
    }

    write_json_output(args.json_out, payload)
    log(
        "Completed successfully: "
        f"mergeable={payload['mergeable']}, merge_commit_sha={payload['merge_commit_sha']}, "
        f"merge_commit_url={payload['merge_commit_url']} ,"
        f"merge_ref_url={payload['merge_ref_url']}"
    )
    if args.get_info:
        # "get-info" is successful if we could produce structured output,
        # even when merge_commit_sha is null (for stale/missing ref cases).
        return 0
    return 0 if mergeable else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ScriptError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
