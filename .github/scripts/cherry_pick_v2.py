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
from typing import List, Optional, Tuple, Any, Set
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
class CherryPickProgress:
    skipped_commit_shas: List[str] = field(default_factory=list)
    applied_commit_shas: List[str] = field(default_factory=list)
    failed_commit_sha: Optional[str] = None


class BranchBackportSkipped(Exception):
    def __init__(self, target_branch: str, progress: CherryPickProgress):
        self.target_branch = target_branch
        self.progress = progress
        super().__init__(f"all commits already present in `{target_branch}`")


class CherryPickFailed(Exception):
    def __init__(self, target_branch: str, message: str, progress: CherryPickProgress):
        self.target_branch = target_branch
        self.progress = progress
        super().__init__(message)


@dataclass
class BranchBackportFailure:
    target_branch: str
    error: str
    progress: CherryPickProgress


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


def is_empty_cherry_pick_output(output: str) -> bool:
    lowered = output.lower()
    return (
        "cherry-pick is now empty" in lowered
        or "nothing to commit" in lowered
    )


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
    
    # Get commit message title (first line)
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
        # Try GraphQL
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
        
        # Fallback to parsing PR body
        if not issues and pull.body:
            issues = [f"#{num}" for num in re.findall(r'#(\d+)', pull.body)]
        
        all_issues.extend(issues)
    
    unique_issues = list(dict.fromkeys(all_issues))
    return ' '.join(unique_issues) if unique_issues else 'None'


def extract_changelog(pr_body: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extracts changelog category, entry, and entry content"""
    if not pr_body:
        return None, None, None
    
    # Category
    category_match = re.search(r"### Changelog category.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
    category = None
    if category_match:
        categories = [line.lstrip('* ').strip() for line in category_match.group(1).splitlines() if line.strip() and line.strip().startswith('*')]
        category = categories[0] if categories else None
    
    # Entry
    entry_match = re.search(r"### Changelog entry.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
    entry = entry_match.group(1).strip() if entry_match else None
    if entry in ['...', '']:
        entry = None
    
    # Entry content (stops at category)
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
    # Collect data
    all_commit_shas = []
    all_pull_requests = []
    all_titles = []
    all_body_items = []
    all_authors = []
    
    for source in sources:
        all_commit_shas.extend(source.commit_shas)
        all_pull_requests.extend(source.pull_requests)
        all_titles.append(source.title)
        all_body_items.append(source.body_item)
        if source.author and source.author not in all_authors:
            all_authors.append(source.author)
    
    # Title
    if len(all_titles) == 1:
        title = f"[Backport {target_branch}] {all_titles[0]}"
    else:
        title = f"[Backport {target_branch}] {', '.join(all_titles)}"
    if has_conflicts:
        title = f"[CONFLICT] {title}"
    if len(title) > 256: # GitHub limit for PR title
        title = title[:253] + "..."
    
    # Issues
    issue_refs = get_linked_issues(repo, token, all_pull_requests, logger)
    authors_str = ', '.join([f"@{a}" for a in set(all_authors)]) if all_authors else "Unknown"
    
    # Changelog: build entry for each source, then merge
    categories = []
    changelog_entries = []
    
    for source in sources:
        source_entry = None
        source_category = None
        
        # For PR or merge commit with linked PR
        if source.pull_requests:
            pull = source.pull_requests[0]
            if pull.body:
                cat, ent, ent_content = extract_changelog(pull.body)
                source_category = cat
                # Use entry_content if available, otherwise entry
                source_entry = ent_content if ent_content else ent
            
            # Format: "PR Title: changelog_entry" or just "PR Title"
            if source_entry:
                changelog_entries.append(f"{pull.title}: {source_entry}")
            else:
                changelog_entries.append(pull.title)
        
        # For commit SHA (no linked PR)
        elif source.type == 'commit' and source.commit_shas:
            try:
                commit = repo.get_commit(source.commit_shas[0])
                commit_message = commit.commit.message
                commit_title = commit_message.split('\n')[0].strip()
                changelog_entries.append(commit_title)
            except Exception as e:
                logger.debug(f"Failed to get commit message for {source.commit_shas[0]}: {e}")
                changelog_entries.append(f"commit {source.commit_shas[0][:7]}")
        
        if source_category:
            categories.append(source_category)
    
    changelog_category = categories[0] if len(set(categories)) == 1 else None
    
    # Merge all entries
    if len(changelog_entries) > 1:
        changelog_entry = "\n\n---\n\n".join(changelog_entries)
    elif len(changelog_entries) == 1:
        changelog_entry = changelog_entries[0]
    else:
        # Fallback
        changelog_entry = f"Backport to `{target_branch}`"
    
    if changelog_category == "Bugfix" and issue_refs != "None":
        if not any(re.search(p, changelog_entry) for p in ISSUE_PATTERNS):
            changelog_entry = f"{changelog_entry} ({issue_refs})"
    
    category_section = f"* {changelog_category}" if changelog_category else get_category_section_template()
    commits = '\n'.join(all_body_items)
    
    # Build body sections
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


def format_source_branch_status(
    source: Source,
    target_branch: str,
    ordered_commit_shas: List[str],
    result: Optional[BackportResult],
    failure: Optional[BranchBackportFailure],
    skip: Optional[BranchBackportSkipped],
    sources: List[Source],
) -> str:
    source_shas = set(source.commit_shas)
    source_indices = [ordered_commit_shas.index(sha) for sha in source.commit_shas]

    if skip and skip.target_branch == target_branch:
        if all(sha in skip.progress.skipped_commit_shas for sha in source.commit_shas):
            return f"`{target_branch}`: already present"

    if failure and failure.target_branch == target_branch:
        progress = failure.progress
        failed_idx = (
            ordered_commit_shas.index(progress.failed_commit_sha)
            if progress.failed_commit_sha in ordered_commit_shas
            else len(ordered_commit_shas)
        )
        max_source_idx = max(source_indices)
        min_source_idx = min(source_indices)

        if progress.failed_commit_sha in source_shas:
            return f"`{target_branch}`: failed ({failure.error})"
        if max_source_idx < failed_idx:
            if all(sha in progress.skipped_commit_shas for sha in source.commit_shas):
                return f"`{target_branch}`: already present"
            if all(sha in progress.applied_commit_shas for sha in source.commit_shas):
                return (
                    f"`{target_branch}`: not backported "
                    f"(workflow failed before backport PR was created)"
                )
        if min_source_idx > failed_idx and progress.failed_commit_sha:
            failed_label = describe_commit(progress.failed_commit_sha, sources)
            return f"`{target_branch}`: not backported (workflow failed on {failed_label})"

    if result and result.target_branch == target_branch:
        if all(sha in result.skipped_commit_shas for sha in source.commit_shas):
            return f"`{target_branch}`: already present"
        if any(sha in result.applied_commit_shas for sha in source.commit_shas) and result.pr:
            status = "draft PR" if result.has_conflicts else "backport PR"
            conflict_note = " (contains conflicts requiring manual resolution)" if result.has_conflicts else ""
            return f"`{target_branch}`: included in {status} {result.pr.html_url}{conflict_note}"

    return f"`{target_branch}`: status unknown"


def build_pull_result_comment(
    pull_number: int,
    sources: List[Source],
    ordered_commit_shas: List[str],
    results: List[BackportResult],
    branch_failures: List[BranchBackportFailure],
    branch_skips: List[BranchBackportSkipped],
    invalid_branches: Set[str],
    target_branches: List[str],
    workflow_url: Optional[str],
) -> str:
    source = get_source_for_pull(pull_number, sources)
    if not source:
        return "Backport completed (no source metadata for this PR)"

    branch_lines = []
    for branch in target_branches:
        if branch in invalid_branches:
            branch_lines.append(f"`{branch}`: skipped (branch does not exist)")
            continue

        result = next((r for r in results if r.target_branch == branch), None)
        failure = next((f for f in branch_failures if f.target_branch == branch), None)
        skip = next((s for s in branch_skips if s.target_branch == branch), None)
        branch_lines.append(format_source_branch_status(
            source, branch, ordered_commit_shas, result, failure, skip, sources,
        ))

    if len(branch_lines) == 1:
        text = f"Backport result for this PR: {branch_lines[0]}"
    else:
        text = "Backport results for this PR:\n" + "\n".join(f"- {line}" for line in branch_lines)

    if workflow_url:
        text += f"\n\n[workflow run]({workflow_url})"
    return text


def update_comments(
    backport_comments: List[Tuple[Any, object]],
    sources: List[Source],
    ordered_commit_shas: List[str],
    results: List[BackportResult],
    branch_failures: List[BranchBackportFailure],
    branch_skips: List[BranchBackportSkipped],
    invalid_branches: Set[str],
    target_branches: List[str],
    workflow_url: Optional[str],
    logger,
):
    """Updates comments with per-PR backport results"""
    if not backport_comments:
        return

    for pull, comment in backport_comments:
        try:
            existing_body = comment.body
            new_results = build_pull_result_comment(
                pull.number,
                sources,
                ordered_commit_shas,
                results,
                branch_failures,
                branch_skips,
                invalid_branches,
                target_branches,
                workflow_url,
            )

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
                updated_lines.append("")
                updated_lines.append(new_results)

            comment.edit('\n'.join(updated_lines))
            logger.info("Updated backport comment in original PR #%s", pull.number)
        except GithubException as e:
            logger.warning("Failed to update comment in original PR #%s: %s", pull.number, e)


def process_branch(
    repo_path: str, target_branch: str, dev_branch_name: str, commit_shas: List[str],
    repo_name: str, repo, token: str, sources: List[Source], workflow_triggerer: str,
    workflow_url: Optional[str], summary_path: Optional[str], logger
):
    """Processes single branch"""
    all_conflict_files = []
    cherry_pick_logs = []
    
    # Prepare branch
    run_git(repo_path, ['fetch', 'origin', target_branch], logger)
    run_git(repo_path, ['reset', '--hard', 'HEAD'], logger)
    run_git(repo_path, ['checkout', '-B', target_branch, f'origin/{target_branch}'], logger)
    run_git(repo_path, ['checkout', '-b', dev_branch_name, target_branch], logger)
    
    progress = CherryPickProgress()

    # Cherry-pick each commit
    for commit_sha in commit_shas:
        commit_label = describe_commit(commit_sha, sources)
        logger.info("Cherry-picking %s (%s)", commit_sha[:7], commit_label)
        run_git(repo_path, ['fetch', 'origin', commit_sha], logger, check=False)
        try:
            result = run_git(repo_path, ['cherry-pick', '--allow-empty', commit_sha], logger, check=False)
            output = (result.stdout or '') + (('\n' + result.stderr) if result.stderr else '')

            if result.returncode != 0:
                if is_empty_cherry_pick_output(output):
                    run_git(repo_path, ['cherry-pick', '--skip'], logger, check=False)
                    skip_msg = (
                        f"{commit_label} ({commit_sha[:7]}) is already present in `{target_branch}`, skipped"
                    )
                    logger.warning("ALREADY_PRESENT: %s", skip_msg)
                    cherry_pick_logs.append(f"=== Skipped {commit_sha[:7]} ===\n{skip_msg}\n")
                    progress.skipped_commit_shas.append(commit_sha)
                    continue

                progress.failed_commit_sha = commit_sha
                if "conflict" in output.lower():
                    conflicts = detect_conflicts(repo_path, logger)
                    if conflicts:
                        run_git(repo_path, ['add', '-A'], logger)
                        run_git(repo_path, ['commit', '-m', f"BACKPORT-CONFLICT: manual resolution required for commit {commit_sha[:7]}"], logger)
                        all_conflict_files.extend(conflicts)
                        progress.applied_commit_shas.append(commit_sha)
                    else:
                        run_git(repo_path, ['cherry-pick', '--abort'], logger, check=False)
                        raise CherryPickFailed(
                            target_branch,
                            f"Cherry-pick failed for {commit_label} ({commit_sha[:7]}): "
                            f"git reported a conflict but no conflicted files were detected.\n{output.strip()}",
                            progress,
                        )
                else:
                    raise CherryPickFailed(
                        target_branch,
                        f"Cherry-pick failed for {commit_label} ({commit_sha[:7]}): {output.strip()}",
                        progress,
                    )
            else:
                progress.applied_commit_shas.append(commit_sha)

            if output and commit_sha not in progress.skipped_commit_shas:
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

    # Push branch
    run_git(repo_path, ['push', '--set-upstream', 'origin', dev_branch_name], logger)
    
    # Create PR
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
    
    # Update body with PR number for correct links
    if has_conflicts:
        _, updated_body = build_pr_content(
            repo_name, repo, token, target_branch, dev_branch_name,
            applied_sources, all_conflict_files, cherry_pick_logs,
            workflow_triggerer, workflow_url, pr.number, logger
        )
        pr.edit(body=updated_body)
    
    # Assign assignee
    if workflow_triggerer != 'unknown':
        try:
            pr.add_to_assignees(workflow_triggerer)
        except GithubException:
            pass
    
    # Enable automerge if no conflicts
    if not has_conflicts:
        try:
            pr.enable_automerge(merge_method='MERGE')
        except Exception:
            try:
                pr.enable_automerge(merge_method='SQUASH')
            except Exception:
                pass
    
    # Write to summary
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
    
    # Initialize GitHub
    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(repo_name)
    
    # Get workflow URL
    workflow_url = None
    run_id = os.getenv('GITHUB_RUN_ID')
    if run_id:
        try:
            workflow_url = repo.get_workflow_run(int(run_id)).html_url
        except (GithubException, ValueError):
            pass
    
    # Parse input
    def split_input(s: str) -> List[str]:
        if not s:
            return []
        pattern = r"[, \n]+"
        return [part.strip() for part in re.split(pattern, s) if part.strip()]
    
    commits = split_input(args.commits)
    target_branches = split_input(args.target_branches)
    allow_unmerged = getattr(args, 'allow_unmerged', False)
    
    # Collect sources
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
            if not pull.merged:
                logger.info(f"PR #{pr_num} is unmerged, using {len(source.commit_shas)} commits from PR")
            elif pull.merge_commit_sha:
                merge_commit = repo.get_commit(pull.merge_commit_sha)
                if merge_commit.parents and len(merge_commit.parents) > 1:
                    logger.info(f"PR #{pr_num} was merged as merge commit, using {len(source.commit_shas)} individual commits")
                else:
                    logger.info(f"PR #{pr_num} was merged as squash/rebase, using merge_commit_sha")
        except ValueError:
            # Not a PR number, treat as commit SHA
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
            
            # Check if commit is linked to PR
            pulls = commit.get_pulls()
            if pulls.totalCount > 0:
                pr = pulls.get_page(0)[0]
                if not pr.merged and not allow_unmerged:
                    raise ValueError(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged. Cannot backport unmerged PR. Use --allow-unmerged to allow")
                if not pr.merged:
                    logger.info(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged, but --allow-unmerged is set, proceeding")
            
            source = create_commit_source(commit, repo, logger)
            sources.append(source)
    
    # Validate
    all_commit_shas = []
    all_pull_requests = []
    for source in sources:
        all_commit_shas.extend(source.commit_shas)
        all_pull_requests.extend(source.pull_requests)
    
    if not all_commit_shas:
        logger.error("VALIDATION_ERROR: No commits to cherry-pick")
        sys.exit(1)
    if not target_branches:
        logger.error("VALIDATION_ERROR: No target branches specified")
        sys.exit(1)
    
    for pull in all_pull_requests:
        if not pull.merged and not allow_unmerged:
            logger.error(f"VALIDATION_ERROR: PR #{pull.number} is not merged. Use --allow-unmerged to allow backporting unmerged PRs")
            sys.exit(1)
    
    for commit_sha in all_commit_shas:
        try:
            repo.get_commit(commit_sha)
        except GithubException as e:
            logger.error(f"VALIDATION_ERROR: Commit {commit_sha} does not exist: {e}")
            sys.exit(1)
    
    # Validate branches and collect invalid ones
    invalid_branches = []
    for branch in target_branches:
        try:
            repo.get_branch(branch)
        except GithubException as e:
            logger.error(f"VALIDATION_ERROR: Branch {branch} does not exist: {e}")
            invalid_branches.append(branch)
    
    # Remove invalid branches from target_branches
    valid_target_branches = [b for b in target_branches if b not in invalid_branches]
    
    if not valid_target_branches:
        logger.error("VALIDATION_ERROR: No valid target branches after validation")
        sys.exit(1)
    
    if invalid_branches:
        logger.warning(f"VALIDATION_WARNING: Skipping invalid branches: {', '.join(invalid_branches)}")
    
    logger.info("Input validation successful")
    
    # Create initial comment
    backport_comments = []
    if all_pull_requests:
        target_branches_str = ', '.join([f"`{b}`" for b in target_branches])
        if workflow_url:
            new_line = f"Backport to {target_branches_str} in progress: [workflow run]({workflow_url})"
        else:
            new_line = f"Backport to {target_branches_str} in progress"
        
        for pull in all_pull_requests:
            try:
                existing_comment = find_existing_backport_comment(pull, logger)
                if existing_comment:
                    existing_body = existing_comment.body
                    branches_already_mentioned = all(f"`{b}`" in existing_body for b in target_branches)
                    should_skip = (
                        branches_already_mentioned and 
                        ("in progress" in existing_body) and
                        (not workflow_url or workflow_url in existing_body)
                    )
                    
                    if should_skip:
                        backport_comments.append((pull, existing_comment))
                    else:
                        existing_comment.edit(f"{existing_body}\n\n{new_line}")
                        backport_comments.append((pull, existing_comment))
                        logger.info(f"Updated existing backport comment in original PR #{pull.number}")
                else:
                    comment = pull.create_issue_comment(new_line)
                    backport_comments.append((pull, comment))
                    logger.info(f"Created initial backport comment in original PR #{pull.number}")
            except GithubException as e:
                logger.warning(f"Failed to create/update initial comment in original PR #{pull.number}: {e}")
    
    # Clone repository
    repo_dir = tempfile.mkdtemp(prefix="ydb-cherry-pick-")
    try:
        repo_url = f"https://{token}@github.com/{repo_name}.git"
        logger.info("Cloning repository: %s to %s", repo_url, repo_dir)
        subprocess.run(
            ['git', 'clone', repo_url, repo_dir],
            env={**os.environ, 'GIT_PROTOCOL': '2'},
            check=True,
            capture_output=True
        )
        
        # Process each target branch
        results: List[BackportResult] = []
        branch_failures: List[BranchBackportFailure] = []
        branch_skips: List[BranchBackportSkipped] = []
        invalid_branch_set = set(invalid_branches)

        has_errors = False
        dtm = datetime.datetime.now().strftime("%y%m%d-%H%M%S")

        for target_branch in valid_target_branches:
            try:
                dev_branch_name = f"cherry-pick-{target_branch}-{dtm}"
                result = process_branch(
                    repo_dir, target_branch, dev_branch_name, all_commit_shas,
                    repo_name, repo, token, sources, workflow_triggerer, workflow_url, summary_path, logger
                )
                results.append(result)
            except BranchBackportSkipped as e:
                logger.info("Branch %s skipped: %s", target_branch, e)
                branch_skips.append(e)
                if summary_path:
                    skipped_labels = ", ".join(
                        describe_commit(sha, sources) for sha in e.progress.skipped_commit_shas
                    )
                    with open(summary_path, 'a') as f:
                        f.write(f"### Branch `{target_branch}`: skipped\n\nAll commits already present: {skipped_labels}\n\n")
            except CherryPickFailed as e:
                has_errors = True
                logger.error("BACKPORT_ERROR: Branch `%s`: %s", target_branch, e)
                branch_failures.append(BranchBackportFailure(target_branch, str(e), e.progress))
                if summary_path:
                    with open(summary_path, 'a') as f:
                        f.write(f"### Branch `{target_branch}`: failed\n\n```\n{e}\n```\n\n")
            except Exception as e:
                has_errors = True
                logger.error("BACKPORT_ERROR: Branch `%s`: %s", target_branch, e)
                branch_failures.append(BranchBackportFailure(
                    target_branch, str(e), CherryPickProgress(failed_commit_sha=all_commit_shas[0] if all_commit_shas else None),
                ))
                if summary_path:
                    with open(summary_path, 'a') as f:
                        f.write(f"### Branch `{target_branch}`: failed\n\n```\n{e}\n```\n\n")

        update_comments(
            backport_comments, sources, all_commit_shas, results,
            branch_failures, branch_skips, invalid_branch_set,
            target_branches, workflow_url, logger,
        )

        if has_errors:
            error_msg = "Cherry-pick workflow completed with errors. See details above."
            logger.error("WORKFLOW_FAILED: %s", error_msg)
            if summary_path:
                with open(summary_path, 'a') as f:
                    f.write(f"**{error_msg}**\n\n")
            sys.exit(1)

        if branch_skips and not results:
            logger.info(
                "WORKFLOW_SUCCESS: Nothing to backport — all commits are already present in target branch(es)."
            )
            if summary_path:
                with open(summary_path, 'a') as f:
                    f.write("Nothing to backport — all commits are already present in target branch(es).\n\n")
        else:
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
        if os.path.exists(repo_dir):
            shutil.rmtree(repo_dir)


if __name__ == "__main__":
    main()
