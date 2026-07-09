#!/usr/bin/env python3
"""
Cherry-pick v2 Script - Automated Backport Tool

Maintains order of input sources and creates PRs with proper metadata.
"""

import os
import sys
import datetime
import logging
import subprocess
import argparse
import re
import tempfile
import shutil
import json
from typing import List, Optional, Tuple, Any, Set, Dict
from dataclasses import dataclass, field
from github import Github, GithubException, Auth
import requests

try:
    pr_template_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'actions', 'validate_pr_description'))
    sys.path.insert(0, pr_template_path)
    from pr_template import ISSUE_PATTERNS, get_category_section_template
except ImportError as e:
    logging.error(f"Failed to import pr_template: {e}")
    raise


@dataclass
class ConflictInfo:
    file_path: str


@dataclass
class Source:
    type: str  # 'commit' or 'pr'
    commit_shas: List[str]
    title: str
    body_item: str
    author: Optional[str]
    pull_requests: List[Any]


@dataclass
class BackportResult:
    target_branch: str
    pr: Optional[Any] = None
    conflict_files: List[ConflictInfo] = field(default_factory=list)
    cherry_pick_logs: List[str] = field(default_factory=list)
    skipped_commit_shas: List[str] = field(default_factory=list)
    applied_commit_shas: List[str] = field(default_factory=list)

    @property
    def has_conflicts(self) -> bool:
        return len(self.conflict_files) > 0


@dataclass
class BackportProgress:
    skipped_commit_shas: List[str] = field(default_factory=list)
    applied_commit_shas: List[str] = field(default_factory=list)
    failed_commit_sha: Optional[str] = None


@dataclass
class BranchPreflight:
    target_branch: str
    already_present_shas: List[str] = field(default_factory=list)
    needs_backport_shas: List[str] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)

    @property
    def all_present(self) -> bool:
        return not self.needs_backport_shas


class BranchBackportSkipped(Exception):
    def __init__(self, target_branch: str, progress: BackportProgress):
        self.target_branch = target_branch
        self.progress = progress
        super().__init__(f"all commits already present in `{target_branch}`")


class CherryPickFailed(Exception):
    def __init__(self, target_branch: str, message: str, progress: BackportProgress):
        self.target_branch = target_branch
        self.progress = progress
        super().__init__(message)


def describe_commit(commit_sha: str, sources: List[Source]) -> str:
    for source in sources:
        if commit_sha in source.commit_shas:
            return source.title
    return f"commit {commit_sha[:7]}"


def get_source_for_pull(pull_number: int, sources: List[Source]) -> Optional[Source]:
    for source in sources:
        for pull in source.pull_requests:
            if pull.number == pull_number:
                return source
    return None


def get_pulls_for_commits(sources: List[Source], commit_shas: List[str]) -> List[Any]:
    pulls = []
    seen = set()
    commit_set = set(commit_shas)
    for source in sources:
        if not any(sha in commit_set for sha in source.commit_shas):
            continue
        for pull in source.pull_requests:
            if pull.number not in seen:
                pulls.append(pull)
                seen.add(pull.number)
    return pulls


def is_empty_cherry_pick_output(output: str) -> bool:
    lowered = output.lower()
    return (
        "cherry-pick is now empty" in lowered
        or "nothing to commit" in lowered
    )


def git_output(result: subprocess.CompletedProcess) -> str:
    output = result.stdout or ''
    if result.stderr:
        output = f"{output}\n{result.stderr}" if output else result.stderr
    return output


@dataclass
class CherryPickOutcome:
    status: str  # already_present, needs_backport, applied, conflict, failed
    output: str = ''
    conflicts: List[ConflictInfo] = field(default_factory=list)


def cherry_pick_commit(
    repo_path: str,
    commit_sha: str,
    logger,
    *,
    preflight: bool = False,
) -> CherryPickOutcome:
    run_git(repo_path, ['fetch', 'origin', commit_sha], logger, check=False)
    cmd = (
        ['cherry-pick', '--no-commit', commit_sha]
        if preflight
        else ['cherry-pick', '--allow-empty', commit_sha]
    )
    result = run_git(repo_path, cmd, logger, check=False)
    output = git_output(result)

    if result.returncode == 0:
        if preflight:
            status = run_git(repo_path, ['status', '--porcelain'], logger, check=False)
            if not status.stdout.strip():
                return CherryPickOutcome('already_present', output)
            return CherryPickOutcome('needs_backport', output)
        return CherryPickOutcome('applied', output)

    if is_empty_cherry_pick_output(output):
        if not preflight:
            run_git(repo_path, ['cherry-pick', '--skip'], logger, check=False)
        return CherryPickOutcome('already_present', output)

    if not preflight and 'conflict' in output.lower():
        conflicts = detect_conflicts(repo_path, logger)
        if conflicts:
            return CherryPickOutcome('conflict', output, conflicts)

    return CherryPickOutcome('failed', output)


def run_git(repo_path: str, cmd: List[str], logger, check=True) -> subprocess.CompletedProcess:
    """Run git command"""
    result = subprocess.run(
        ['git'] + cmd,
        cwd=repo_path,
        capture_output=True,
        text=True,
        check=check
    )
    return result


def reset_branch_to_target(repo_path: str, target_branch: str, logger) -> None:
    run_git(repo_path, ['cherry-pick', '--abort'], logger, check=False)
    run_git(repo_path, ['reset', '--hard', f'origin/{target_branch}'], logger)
    run_git(repo_path, ['clean', '-fd'], logger, check=False)


def setup_branch_repo(token: str, repo_name: str, target_branch: str, logger) -> str:
    repo_dir = tempfile.mkdtemp(prefix="ydb-cherry-pick-")
    repo_url = f"https://{token}@github.com/{repo_name}.git"
    logger.info("Cloning branch `%s` to %s", target_branch, repo_dir)
    subprocess.run(
        ['git', 'clone', '--depth=1', '--branch', target_branch, repo_url, repo_dir],
        env={**os.environ, 'GIT_PROTOCOL': '2'},
        check=True,
        capture_output=True,
        text=True,
    )
    return repo_dir


def classify_commits_for_branch(
    repo_path: str,
    target_branch: str,
    commit_shas: List[str],
    sources: List[Source],
    logger,
) -> BranchPreflight:
    run_git(repo_path, ['fetch', 'origin', target_branch], logger, check=False)
    run_git(
        repo_path,
        ['checkout', '-B', f'preflight-{target_branch}', f'origin/{target_branch}'],
        logger,
    )

    already_present: List[str] = []
    needs_backport: List[str] = []
    logs: List[str] = []

    for commit_sha in commit_shas:
        commit_label = describe_commit(commit_sha, sources)
        outcome = cherry_pick_commit(repo_path, commit_sha, logger, preflight=True)

        if outcome.status == 'already_present':
            already_present.append(commit_sha)
            msg = f"{commit_label} ({commit_sha[:7]}) already present in `{target_branch}`"
            logger.info("PREFLIGHT: %s", msg)
            logs.append(msg)
        else:
            needs_backport.append(commit_sha)
            logger.info("PREFLIGHT: %s needs backport to `%s`", commit_label, target_branch)

        reset_branch_to_target(repo_path, target_branch, logger)

    return BranchPreflight(
        target_branch=target_branch,
        already_present_shas=already_present,
        needs_backport_shas=needs_backport,
        logs=logs,
    )


def write_preflight_summary(
    preflights: List[BranchPreflight],
    sources: List[Source],
    summary_path: Optional[str],
    logger,
) -> None:
    if not summary_path:
        return
    with open(summary_path, 'a') as f:
        f.write("### Pre-flight check\n\n")
        for preflight in preflights:
            if preflight.all_present:
                f.write(f"- `{preflight.target_branch}`: all commits already present\n")
            elif preflight.already_present_shas:
                f.write(
                    f"- `{preflight.target_branch}`: will backport {len(preflight.needs_backport_shas)} commit(s); "
                    f"{len(preflight.already_present_shas)} already present\n"
                )
                for line in preflight.logs:
                    f.write(f"  - {line}\n")
            else:
                f.write(f"- `{preflight.target_branch}`: will backport {len(preflight.needs_backport_shas)} commit(s)\n")
        f.write("\n")


def expand_sha(repo, ref: str, logger) -> str:
    """Expands short SHA to full SHA using GitHub API"""
    try:
        commits = repo.get_commits(sha=ref)
        if commits.totalCount > 0:
            commit = commits[0]
            if len(ref) == 40:
                return commit.sha if commit.sha == ref else ref
            elif commit.sha.startswith(ref):
                return commit.sha
    except Exception as e:
        logger.debug(f"Failed to find commit via GitHub API: {e}")
    raise ValueError(f"Failed to find commit for '{ref}'")


def create_commit_source(commit, repo, logger) -> Source:
    """Creates source from commit SHA"""
    linked_pr = None
    try:
        pulls = commit.get_pulls()
        if pulls.totalCount > 0:
            linked_pr = pulls.get_page(0)[0]
    except Exception:
        pass

    author = linked_pr.user.login if linked_pr else (commit.author.login if commit.author else None)
    body_item = f"* commit {commit.html_url}: {linked_pr.title}" if linked_pr else f"* commit {commit.html_url}"

    commit_title = commit.commit.message.split('\n')[0].strip() if commit.commit.message else f"commit {commit.sha[:7]}"

    return Source(
        type='commit',
        commit_shas=[commit.sha],
        title=f'commit {commit.sha[:7]}: {commit_title}',
        body_item=body_item,
        author=author,
        pull_requests=[linked_pr] if linked_pr else []
    )


def create_pr_source(pull: Any, allow_unmerged: bool, logger) -> Source:
    """Creates source from PR"""
    if not pull.merged:
        commit_shas = [c.sha for c in pull.get_commits()]
        if not commit_shas:
            raise ValueError(f"PR #{pull.number} contains no commits to cherry-pick")
    elif pull.merge_commit_sha:
        commit_shas = [pull.merge_commit_sha]
    else:
        commit_shas = [c.sha for c in pull.get_commits()]
        if not commit_shas:
            raise ValueError(f"PR #{pull.number} contains no commits to cherry-pick")

    return Source(
        type='pr',
        commit_shas=commit_shas,
        title=f'PR #{pull.number}: {pull.title}',
        body_item=f"* PR {pull.html_url}",
        author=pull.user.login,
        pull_requests=[pull]
    )


def detect_conflicts(repo_path: str, logger) -> List[ConflictInfo]:
    """Detects conflicts from git status"""
    conflict_files = []
    CONFLICT_STATUS_CODES = ['UU', 'AA', 'DD', 'DU', 'UD', 'AU', 'UA']

    try:
        result = run_git(repo_path, ['status', '--porcelain'], logger)
        if not result.stdout.strip():
            return []

        for line in result.stdout.strip().split('\n'):
            status_code = line[:2]
            if status_code in CONFLICT_STATUS_CODES:
                parts = line.split(None, 1)
                if len(parts) > 1:
                    file_path = parts[1].strip()
                    if file_path:
                        conflict_files.append(ConflictInfo(file_path=file_path))
    except Exception as e:
        logger.error(f"Error detecting conflicts: {e}")

    return conflict_files


def get_linked_issues(repo, token: str, pull_requests: List[Any], logger) -> str:
    """Gets linked issues for all PRs"""
    all_issues = []
    owner, repo_name = repo.full_name.split('/')

    for pull in pull_requests:
        issues = []
        try:
            query = """
            query($owner: String!, $repo: String!, $prNumber: Int!) {
              repository(owner: $owner, name: $repo) {
                pullRequest(number: $prNumber) {
                  closingIssuesReferences(first: 100) {
                    nodes {
                      number
                      repository {
                        owner { login }
                        name
                      }
                    }
                  }
                }
              }
            }
            """
            response = requests.post(
                "https://api.github.com/graphql",
                headers={"Authorization": f"token {token}"},
                json={"query": query, "variables": {"owner": owner, "repo": repo_name, "prNumber": pull.number}},
                timeout=10
            )
            if response.ok:
                data = response.json()
                nodes = data.get("data", {}).get("repository", {}).get("pullRequest", {}).get("closingIssuesReferences", {}).get("nodes", [])
                for issue in nodes:
                    owner_name = issue["repository"]["owner"]["login"]
                    repo_name_issue = issue["repository"]["name"]
                    number = issue["number"]
                    if owner_name == owner and repo_name_issue == repo_name:
                        issues.append(f"#{number}")
                    else:
                        issues.append(f"{owner_name}/{repo_name_issue}#{number}")
        except Exception:
            pass

        if not issues and pull.body:
            issues = [f"#{num}" for num in re.findall(r'#(\d+)', pull.body)]

        all_issues.extend(issues)

    unique_issues = list(dict.fromkeys(all_issues))
    return ' '.join(unique_issues) if unique_issues else 'None'


def extract_changelog(pr_body: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extracts changelog category, entry, and entry content"""
    if not pr_body:
        return None, None, None

    category_match = re.search(r"### Changelog category.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
    category = None
    if category_match:
        categories = [line.lstrip('* ').strip() for line in category_match.group(1).splitlines() if line.strip() and line.strip().startswith('*')]
        category = categories[0] if categories else None

    entry_match = re.search(r"### Changelog entry.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
    entry = entry_match.group(1).strip() if entry_match else None
    if entry in ['...', '']:
        entry = None

    entry_content_match = re.search(r"### Changelog entry.*?\n(.*?)(\n### Changelog category|$)", pr_body, re.DOTALL)
    entry_content = entry_content_match.group(1).strip() if entry_content_match else None
    if entry_content in ['...', '']:
        entry_content = None

    return category, entry, entry_content


def build_pr_content(
    repo_name: str, repo, token: str, target_branch: str, dev_branch_name: str,
    sources: List[Source], conflict_files: List[ConflictInfo],
    cherry_pick_logs: List[str], workflow_triggerer: str, workflow_url: Optional[str],
    pr_number: Optional[int], logger
) -> Tuple[str, str]:
    """Generates PR title and body"""
    has_conflicts = len(conflict_files) > 0
    all_titles = []
    all_body_items = []
    all_authors = []
    all_pull_requests = []

    for source in sources:
        all_pull_requests.extend(source.pull_requests)
        all_titles.append(source.title)
        all_body_items.append(source.body_item)
        if source.author and source.author not in all_authors:
            all_authors.append(source.author)

    if len(all_titles) == 1:
        title = f"[Backport {target_branch}] {all_titles[0]}"
    else:
        title = f"[Backport {target_branch}] {', '.join(all_titles)}"
    if has_conflicts:
        title = f"[CONFLICT] {title}"
    if len(title) > 256:
        title = title[:253] + "..."

    issue_refs = get_linked_issues(repo, token, all_pull_requests, logger)
    authors_str = ', '.join([f"@{a}" for a in set(all_authors)]) if all_authors else "Unknown"

    categories = []
    changelog_entries = []

    for source in sources:
        source_entry = None
        source_category = None

        if source.pull_requests:
            pull = source.pull_requests[0]
            if pull.body:
                cat, ent, ent_content = extract_changelog(pull.body)
                source_category = cat
                source_entry = ent_content if ent_content else ent

            if source_entry:
                changelog_entries.append(f"{pull.title}: {source_entry}")
            else:
                changelog_entries.append(pull.title)
        elif source.type == 'commit' and source.commit_shas:
            try:
                commit = repo.get_commit(source.commit_shas[0])
                commit_title = commit.commit.message.split('\n')[0].strip()
                changelog_entries.append(commit_title)
            except Exception as e:
                logger.debug(f"Failed to get commit message for {source.commit_shas[0]}: {e}")
                changelog_entries.append(f"commit {source.commit_shas[0][:7]}")

        if source_category:
            categories.append(source_category)

    changelog_category = categories[0] if len(set(categories)) == 1 else None

    if len(changelog_entries) > 1:
        changelog_entry = "\n\n---\n\n".join(changelog_entries)
    elif len(changelog_entries) == 1:
        changelog_entry = changelog_entries[0]
    else:
        changelog_entry = f"Backport to `{target_branch}`"

    if changelog_category == "Bugfix" and issue_refs != "None":
        if not any(re.search(p, changelog_entry) for p in ISSUE_PATTERNS):
            changelog_entry = f"{changelog_entry} ({issue_refs})"

    category_section = f"* {changelog_category}" if changelog_category else get_category_section_template()
    commits = '\n'.join(all_body_items)

    description = f"#### Original PR(s)\n{commits}\n\n#### Metadata\n"
    description += f"- **Original PR author(s):** {authors_str}\n"
    description += f"- **Cherry-picked by:** @{workflow_triggerer}\n"
    description += f"- **Related issues:** {issue_refs}"

    cherry_pick_log_section = ""
    if cherry_pick_logs:
        cherry_pick_log_section = "\n\n### Git Cherry-Pick Log\n\n```\n" + '\n'.join(log if log.endswith('\n') else log + '\n' for log in cherry_pick_logs) + "```\n"

    conflicts_section = ""
    if has_conflicts:
        branch_for_instructions = dev_branch_name or target_branch
        conflicts_section = "\n\n#### Conflicts Require Manual Resolution\n\n"
        conflicts_section += "This PR contains merge conflicts that require manual resolution.\n\n"
        if conflict_files:
            conflicts_section += "**Files with conflicts:**\n\n"
            for conflict in conflict_files:
                file_link = f"https://github.com/{repo_name}/pull/{pr_number}/files" if pr_number else f"https://github.com/{repo_name}/blob/{branch_for_instructions}/{conflict.file_path}"
                conflicts_section += f"- [{conflict.file_path}]({file_link})\n"
        conflicts_section += f"""
**How to resolve conflicts:**

```bash
git fetch origin
git checkout --track origin/{branch_for_instructions}
# Resolve conflicts in files
git add .
git commit -m "Resolved merge conflicts"
git push
```

After resolving conflicts:
1. Fix the PR title (remove `[CONFLICT]` if conflicts are resolved)
2. Mark PR as ready for review
"""

    workflow_section = f"\n\n---\n\nPR was created by cherry-pick workflow [run]({workflow_url})" if workflow_url else "\n\n---\n\nPR was created by cherry-pick script"

    body = f"""### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

{changelog_entry}

### Changelog category <!-- remove all except one -->

{category_section}

### Description for reviewers <!-- (optional) description for those who read this PR -->

{description}{conflicts_section}{cherry_pick_log_section}{workflow_section}
"""

    return title, body


def find_existing_backport_comment(pull: Any, logger):
    """Finds existing backport comment"""
    try:
        for comment in pull.get_issue_comments():
            if comment.user.login == "YDBot" and "Backport" in comment.body and "in progress" in comment.body:
                return comment
    except Exception:
        pass
    return None


def collect_unique_pulls(sources: List[Source]) -> List[Any]:
    pulls = []
    seen = set()
    for source in sources:
        for pull in source.pull_requests:
            if pull.number not in seen:
                pulls.append(pull)
                seen.add(pull.number)
    return pulls


def build_shared_skip_comment(
    pull_number: Optional[int],
    sources: List[Source],
    preflights: Dict[str, BranchPreflight],
    invalid_branches: Set[str],
    target_branches: List[str],
    workflow_url: Optional[str],
    cancelled_entire_run: bool,
) -> str:
    source = get_source_for_pull(pull_number, sources) if pull_number else None
    branch_lines = []
    for branch in target_branches:
        if branch in invalid_branches:
            branch_lines.append(f"`{branch}`: skipped (branch does not exist)")
            continue

        preflight = preflights.get(branch)
        if not preflight:
            continue
        if preflight.all_present:
            branch_lines.append(f"`{branch}`: skipped (all commits already present)")
        elif source and all(sha in preflight.already_present_shas for sha in source.commit_shas):
            branch_lines.append(f"`{branch}`: skipped (already present)")

    if cancelled_entire_run:
        if len(branch_lines) == 1:
            text = f"Backport cancelled: {branch_lines[0]}"
        else:
            text = "Backport cancelled:\n" + "\n".join(f"- {line}" for line in branch_lines)
    elif len(branch_lines) == 1:
        text = f"Backport skipped: {branch_lines[0]}"
    else:
        text = "Backport skipped:\n" + "\n".join(f"- {line}" for line in branch_lines)

    if workflow_url:
        text += f"\n\n[workflow run]({workflow_url})"
    return text


def notify_already_present_pulls(
    pulls: List[Any],
    sources: List[Source],
    preflights: Dict[str, BranchPreflight],
    invalid_branches: Set[str],
    target_branches: List[str],
    workflow_url: Optional[str],
    cancelled_entire_run: bool,
    logger,
):
    for pull in pulls:
        message = build_shared_skip_comment(
            pull.number,
            sources,
            preflights,
            invalid_branches,
            target_branches,
            workflow_url,
            cancelled_entire_run,
        )
        try:
            pull.create_issue_comment(message)
            logger.info("Posted skip backport comment on PR #%s", pull.number)
        except GithubException as e:
            logger.warning("Failed to post skip backport comment on PR #%s: %s", pull.number, e)


def create_initial_backport_comments(
    pulls: List[Any],
    target_branches: List[str],
    workflow_url: Optional[str],
    logger,
) -> List[Tuple[Any, object]]:
    backport_comments = []
    if not pulls:
        return backport_comments

    target_branches_str = ', '.join([f"`{b}`" for b in target_branches])
    if workflow_url:
        new_line = f"Backport to {target_branches_str} in progress: [workflow run]({workflow_url})"
    else:
        new_line = f"Backport to {target_branches_str} in progress"

    for pull in pulls:
        try:
            existing_comment = find_existing_backport_comment(pull, logger)
            if existing_comment:
                existing_body = existing_comment.body
                branches_already_mentioned = all(f"`{b}`" in existing_body for b in target_branches)
                should_skip = (
                    branches_already_mentioned
                    and ("in progress" in existing_body)
                    and (not workflow_url or workflow_url in existing_body)
                )

                if should_skip:
                    backport_comments.append((pull, existing_comment))
                else:
                    existing_comment.edit(f"{existing_body}\n\n{new_line}")
                    backport_comments.append((pull, existing_comment))
                    logger.info("Updated existing backport comment in original PR #%s", pull.number)
            else:
                comment = pull.create_issue_comment(new_line)
                backport_comments.append((pull, comment))
                logger.info("Created initial backport comment in original PR #%s", pull.number)
        except GithubException as e:
            logger.warning("Failed to create/update initial comment in original PR #%s: %s", pull.number, e)

    return backport_comments


def replace_progress_line(
    existing_body: str,
    new_results: str,
    target_branches: List[str],
    workflow_url: Optional[str],
) -> str:
    lines = existing_body.split('\n')
    updated_lines = []
    found = False

    for line in lines:
        is_progress_line = (
            "in progress" in line
            and any(f"`{b}`" in line for b in target_branches)
            and (not workflow_url or workflow_url in line)
        )

        if is_progress_line and not found:
            updated_lines.append(new_results)
            found = True
        else:
            updated_lines.append(line)

    if not found:
        updated_lines.extend(["", new_results])

    return '\n'.join(updated_lines)


def update_comments(
    backport_comments: List[Tuple[Any, object]],
    results: List[BackportResult],
    skipped_branches: List[Tuple[str, str]],
    target_branches: List[str],
    workflow_url: Optional[str],
    logger,
):
    """Updates comments with shared backport results"""
    if not backport_comments:
        return

    for pull, comment in backport_comments:
        try:
            new_results = build_shared_result_comment(
                results, skipped_branches, target_branches, workflow_url,
            )
            comment.edit(replace_progress_line(
                comment.body, new_results, target_branches, workflow_url,
            ))
            logger.info("Updated backport comment in original PR #%s", pull.number)
        except GithubException as e:
            logger.warning("Failed to update comment in original PR #%s: %s", pull.number, e)


def build_shared_result_comment(
    results: List[BackportResult],
    skipped_branches: List[Tuple[str, str]],
    target_branches: List[str],
    workflow_url: Optional[str],
) -> str:
    total_branches = len(results) + len(skipped_branches)

    if total_branches == 0:
        new_results = f"Backport to {', '.join([f'`{b}`' for b in target_branches])} completed with no results"
        if workflow_url:
            new_results += f" - [workflow run]({workflow_url})"
    elif total_branches == 1 and len(results) == 1:
        result = results[0]
        if result.pr:
            status = "draft PR" if result.has_conflicts else "PR"
            new_results = f"Backported to `{result.target_branch}`: {status} {result.pr.html_url}"
            if result.has_conflicts:
                new_results += " (contains conflicts requiring manual resolution)"
            if workflow_url:
                new_results += f" - [workflow run]({workflow_url})"
        else:
            new_results = f"Backported to `{result.target_branch}`: failed"
            if workflow_url:
                new_results += f" - [workflow run]({workflow_url})"
    else:
        new_results = "Backport results:\n"
        for result in results:
            if result.pr:
                status = "draft PR" if result.has_conflicts else "PR"
                conflict_note = " (contains conflicts requiring manual resolution)" if result.has_conflicts else ""
                new_results += f"- `{result.target_branch}`: {status} {result.pr.html_url}{conflict_note}\n"
            else:
                new_results += f"- `{result.target_branch}`: failed\n"
        for target_branch, reason in skipped_branches:
            new_results += f"- `{target_branch}`: skipped ({reason})\n"
        if workflow_url:
            new_results += f"\n[workflow run]({workflow_url})"

    return new_results


def process_branch(
    repo_path: str,
    target_branch: str,
    dev_branch_name: str,
    commit_shas: List[str],
    preflight: BranchPreflight,
    repo_name: str,
    repo,
    token: str,
    sources: List[Source],
    workflow_triggerer: str,
    workflow_url: Optional[str],
    summary_path: Optional[str],
    logger,
):
    """Processes single branch"""
    all_conflict_files = []
    cherry_pick_logs = []
    if preflight.logs:
        cherry_pick_logs.append("=== Pre-flight ===\n" + '\n'.join(preflight.logs) + "\n")

    run_git(repo_path, ['fetch', 'origin', target_branch], logger)
    run_git(repo_path, ['reset', '--hard', 'HEAD'], logger)
    run_git(repo_path, ['checkout', '-B', target_branch, f'origin/{target_branch}'], logger)
    run_git(repo_path, ['checkout', '-b', dev_branch_name, target_branch], logger)

    progress = BackportProgress(skipped_commit_shas=list(preflight.already_present_shas))

    for commit_sha in commit_shas:
        commit_label = describe_commit(commit_sha, sources)
        logger.info("Cherry-picking %s (%s)", commit_sha[:7], commit_label)
        try:
            outcome = cherry_pick_commit(repo_path, commit_sha, logger)
            output = outcome.output

            if outcome.status == 'already_present':
                skip_msg = (
                    f"{commit_label} ({commit_sha[:7]}) is already present in `{target_branch}`, skipped"
                )
                logger.warning("ALREADY_PRESENT: %s", skip_msg)
                cherry_pick_logs.append(f"=== Skipped {commit_sha[:7]} ===\n{skip_msg}\n")
                progress.skipped_commit_shas.append(commit_sha)
                continue

            if outcome.status == 'failed':
                progress.failed_commit_sha = commit_sha
                run_git(repo_path, ['cherry-pick', '--abort'], logger, check=False)
                if 'conflict' in output.lower():
                    detail = (
                        f"git reported a conflict but no conflicted files were detected.\n{output.strip()}"
                    )
                else:
                    detail = output.strip()
                raise CherryPickFailed(
                    target_branch,
                    f"Cherry-pick failed for {commit_label} ({commit_sha[:7]}): {detail}",
                    progress,
                )

            if outcome.status == 'conflict':
                run_git(repo_path, ['add', '-A'], logger)
                run_git(
                    repo_path,
                    ['commit', '-m', f"BACKPORT-CONFLICT: manual resolution required for commit {commit_sha[:7]}"],
                    logger,
                )
                all_conflict_files.extend(outcome.conflicts)
                progress.applied_commit_shas.append(commit_sha)
            else:
                progress.applied_commit_shas.append(commit_sha)

            if output:
                cherry_pick_logs.append(f"=== Cherry-picking {commit_sha[:7]} ===\n{output}")
        except subprocess.CalledProcessError as e:
            progress.failed_commit_sha = commit_sha
            raise CherryPickFailed(
                target_branch,
                f"Cherry-pick failed for {commit_label} ({commit_sha[:7]}): {e}",
                progress,
            )

    if not progress.applied_commit_shas:
        raise BranchBackportSkipped(target_branch, progress)

    applied_sources = [
        source for source in sources
        if any(sha in progress.applied_commit_shas for sha in source.commit_shas)
    ]

    run_git(repo_path, ['push', '--set-upstream', 'origin', dev_branch_name], logger)

    has_conflicts = len(all_conflict_files) > 0
    title, body = build_pr_content(
        repo_name, repo, token, target_branch, dev_branch_name,
        applied_sources, all_conflict_files, cherry_pick_logs,
        workflow_triggerer, workflow_url, None, logger
    )

    pr = repo.create_pull(
        base=target_branch,
        head=dev_branch_name,
        title=title,
        body=body,
        maintainer_can_modify=True,
        draft=has_conflicts
    )

    if has_conflicts:
        _, updated_body = build_pr_content(
            repo_name, repo, token, target_branch, dev_branch_name,
            applied_sources, all_conflict_files, cherry_pick_logs,
            workflow_triggerer, workflow_url, pr.number, logger
        )
        pr.edit(body=updated_body)

    if workflow_triggerer != 'unknown':
        try:
            pr.add_to_assignees(workflow_triggerer)
        except GithubException:
            pass

    if not has_conflicts:
        try:
            pr.enable_automerge(merge_method='MERGE')
        except Exception:
            try:
                pr.enable_automerge(merge_method='SQUASH')
            except Exception:
                pass

    if summary_path:
        summary = f"### Branch `{target_branch}`: "
        summary += f"**CONFLICT** Draft PR {pr.html_url}\n\n" if has_conflicts else f"PR {pr.html_url}\n\n"
        if cherry_pick_logs:
            summary += "**Git Cherry-Pick Log:**\n\n```\n" + '\n'.join(cherry_pick_logs) + "```\n\n"
        if has_conflicts and all_conflict_files:
            summary += "**Files with conflicts:**\n\n"
            for conflict in all_conflict_files:
                summary += f"- `{conflict.file_path}`\n"
        with open(summary_path, 'a') as f:
            f.write(f'{summary}\n\n')

    return BackportResult(
        target_branch=target_branch,
        pr=pr,
        conflict_files=all_conflict_files,
        cherry_pick_logs=cherry_pick_logs,
        skipped_commit_shas=progress.skipped_commit_shas,
        applied_commit_shas=progress.applied_commit_shas,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commits", help="List of commits to cherry-pick. Can be SHA, PR number or URL. Separated by space, comma or line end.")
    parser.add_argument("--target-branches", help="List of branches to cherry-pick. Separated by space, comma or line end.")
    parser.add_argument("--allow-unmerged", action='store_true', help="Allow backporting unmerged PRs")
    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=logging.DEBUG)
    logger = logging.getLogger("cherry-pick")

    repo_name = os.environ["REPO"]
    token = os.environ["TOKEN"]
    workflow_triggerer = os.environ.get('GITHUB_ACTOR', 'unknown')
    summary_path = os.getenv('GITHUB_STEP_SUMMARY')
    results_path = os.getenv('RESULTS_PATH')

    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(repo_name)

    workflow_url = None
    run_id = os.getenv('GITHUB_RUN_ID')
    if run_id:
        try:
            workflow_url = repo.get_workflow_run(int(run_id)).html_url
        except (GithubException, ValueError):
            pass

    def split_input(s: str) -> List[str]:
        if not s:
            return []
        pattern = r"[, \n]+"
        return [part.strip() for part in re.split(pattern, s) if part.strip()]

    commits = split_input(args.commits)
    target_branches = split_input(args.target_branches)
    allow_unmerged = getattr(args, 'allow_unmerged', False)

    sources = []
    for c in commits:
        ref = c.split('/')[-1].strip()
        try:
            pr_num = int(ref)
            try:
                pull = repo.get_pull(pr_num)
            except GithubException as e:
                logger.error(f"VALIDATION_ERROR: PR #{pr_num} does not exist: {e}")
                sys.exit(1)

            if not pull.merged and not allow_unmerged:
                raise ValueError(f"PR #{pr_num} is not merged. Use --allow-unmerged to backport unmerged PRs")
            if not pull.merged:
                logger.info(f"PR #{pr_num} is not merged, but --allow-unmerged is set, proceeding with commits from PR")
            source = create_pr_source(pull, allow_unmerged, logger)
            sources.append(source)
        except ValueError:
            try:
                expanded_sha = expand_sha(repo, ref, logger)
            except ValueError as e:
                logger.error(f"VALIDATION_ERROR: Failed to expand SHA {ref}: {e}")
                sys.exit(1)

            try:
                commit = repo.get_commit(expanded_sha)
            except GithubException as e:
                logger.error(f"VALIDATION_ERROR: Commit {ref} (expanded to {expanded_sha}) does not exist: {e}")
                sys.exit(1)

            pulls = commit.get_pulls()
            if pulls.totalCount > 0:
                pr = pulls.get_page(0)[0]
                if not pr.merged and not allow_unmerged:
                    raise ValueError(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged. Cannot backport unmerged PR. Use --allow-unmerged to allow")
            source = create_commit_source(commit, repo, logger)
            sources.append(source)

    all_commit_shas = []
    for source in sources:
        all_commit_shas.extend(source.commit_shas)
    unique_pull_requests = collect_unique_pulls(sources)

    if not all_commit_shas:
        logger.error("VALIDATION_ERROR: No commits to cherry-pick")
        sys.exit(1)
    if not target_branches:
        logger.error("VALIDATION_ERROR: No target branches specified")
        sys.exit(1)

    invalid_branches = []
    for branch in target_branches:
        try:
            repo.get_branch(branch)
        except GithubException as e:
            logger.error(f"VALIDATION_ERROR: Branch {branch} does not exist: {e}")
            invalid_branches.append(branch)

    valid_target_branches = [b for b in target_branches if b not in invalid_branches]
    if not valid_target_branches:
        logger.error("VALIDATION_ERROR: No valid target branches after validation")
        sys.exit(1)

    if invalid_branches:
        logger.warning(f"VALIDATION_WARNING: Skipping invalid branches: {', '.join(invalid_branches)}")

    logger.info("Input validation successful")

    preflights: Dict[str, BranchPreflight] = {}
    repo_dirs: Dict[str, str] = {}
    pulls_to_notify: Dict[int, Any] = {}

    try:
        for target_branch in valid_target_branches:
            repo_dir = setup_branch_repo(token, repo_name, target_branch, logger)
            repo_dirs[target_branch] = repo_dir
            preflight = classify_commits_for_branch(
                repo_dir, target_branch, all_commit_shas, sources, logger,
            )
            preflights[target_branch] = preflight
            if not preflight.all_present:
                for pull in get_pulls_for_commits(sources, preflight.needs_backport_shas):
                    pulls_to_notify[pull.number] = pull

        write_preflight_summary(list(preflights.values()), sources, summary_path, logger)

        has_work = bool(pulls_to_notify)

        if not has_work:
            notify_already_present_pulls(
                unique_pull_requests,
                sources,
                preflights,
                set(invalid_branches),
                target_branches,
                workflow_url,
                True,
                logger,
            )
            logger.info("WORKFLOW_SUCCESS: Nothing to backport — all commits are already present in target branch(es).")
            if summary_path:
                with open(summary_path, 'a') as f:
                    f.write("Nothing to backport — posted skip reason to source PRs.\n\n")
            return

        backport_comments = create_initial_backport_comments(
            list(pulls_to_notify.values()), target_branches, workflow_url, logger,
        )

        results: List[BackportResult] = []
        skipped_branches = [(branch, "branch does not exist") for branch in invalid_branches]
        has_errors = False
        dtm = datetime.datetime.now().strftime("%y%m%d-%H%M%S")

        for target_branch in valid_target_branches:
            preflight = preflights[target_branch]
            if preflight.all_present:
                skipped_branches.append((target_branch, "all commits already present"))
                continue

            try:
                dev_branch_name = f"cherry-pick-{target_branch}-{dtm}"
                result = process_branch(
                    repo_dirs[target_branch], target_branch, dev_branch_name,
                    preflight.needs_backport_shas, preflight,
                    repo_name, repo, token, sources, workflow_triggerer,
                    workflow_url, summary_path, logger,
                )
                results.append(result)
            except BranchBackportSkipped as e:
                logger.info("Branch %s skipped: %s", target_branch, e)
                skipped_branches.append((target_branch, str(e)))
            except CherryPickFailed as e:
                has_errors = True
                logger.error("BACKPORT_ERROR: Branch `%s`: %s", target_branch, e)
                skipped_branches.append((target_branch, str(e)))
                if summary_path:
                    with open(summary_path, 'a') as f:
                        f.write(f"### Branch `{target_branch}`: failed\n\n```\n{e}\n```\n\n")
            except Exception as e:
                has_errors = True
                logger.error("BACKPORT_ERROR: Branch `%s`: %s", target_branch, e)
                skipped_branches.append((target_branch, str(e)))
                if summary_path:
                    with open(summary_path, 'a') as f:
                        f.write(f"### Branch `{target_branch}`: failed\n\n```\n{e}\n```\n\n")

        update_comments(
            backport_comments, results, skipped_branches, target_branches, workflow_url, logger,
        )

        already_present_pulls = [
            pull for pull in unique_pull_requests if pull.number not in pulls_to_notify
        ]
        if already_present_pulls:
            notify_already_present_pulls(
                already_present_pulls,
                sources,
                preflights,
                set(invalid_branches),
                target_branches,
                workflow_url,
                False,
                logger,
            )

        if has_errors:
            error_msg = "Cherry-pick workflow completed with errors. See details above."
            logger.error("WORKFLOW_FAILED: %s", error_msg)
            if summary_path:
                with open(summary_path, 'a') as f:
                    f.write(f"**{error_msg}**\n\n")
            sys.exit(1)

        logger.info("WORKFLOW_SUCCESS: Cherry-pick workflow completed successfully")
        if summary_path:
            with open(summary_path, 'a') as f:
                f.write("Cherry-pick workflow completed successfully\n\n")
        if results_path:
            with open(results_path, 'w') as f:
                json.dump(
                    [{'pr_id': r.pr.id, 'pr_number': r.pr.number, 'branch': r.target_branch} for r in results if r.pr],
                    f,
                    indent=2,
                )
    finally:
        for repo_dir in repo_dirs.values():
            if os.path.exists(repo_dir):
                shutil.rmtree(repo_dir)


if __name__ == "__main__":
    main()
