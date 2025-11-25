#!/usr/bin/env python3
"""
Cherry-pick v2 Script - Automated Backport Tool

ARCHITECTURE OVERVIEW
=====================

This module implements an automated cherry-pick workflow for backporting commits/PRs
to target branches. It maintains order of input sources and creates PRs with proper
metadata.

Main Components:
----------------

1. Data Models
   - ConflictInfo: Information about merge conflicts
   - CherryPickResult: Result of git cherry-pick operation
   - AbstractCommit/CommitSource/PRSource: Source abstraction preserving order
     * CommitSource: Represents a single commit SHA
     * PRSource: Represents a Pull Request (uses merge_commit_sha for merged PRs)
   - SourceInfo: Container for all sources with ordered properties
   - PRContext: Context for PR body generation
   - BackportResult: Result for a single target branch

2. Core Classes
   - CommitResolver: Expands and validates commit references (SHA, short SHA)
   - GitRepository: Git operations wrapper using GitPython
   - ConflictHandler: Detects and handles merge conflicts
   - GitHubClient: GitHub API operations (PR creation, issues)
   - PRContentBuilder: Generates PR title and body with changelog
   - CommentManager: Manages comments on original PRs
   - SummaryWriter: Writes GitHub Actions summary
   - InputParser: Parses and normalizes input strings
   - InputValidator: Validates PRs, commits, and branches

3. Orchestrator
   - CherryPickOrchestrator: Main coordinator class
     * Parses input (PR numbers, commit SHAs)
     * Creates AbstractCommit sources preserving order
     * Processes each target branch:
       - Clones repository
       - Cherry-picks commits in order (git cherry-pick --allow-empty)
       - Handles conflicts
       - Creates PR with proper metadata

Execution Flow:
---------------

1. Parse input: Split commits string, normalize references
2. Create sources: For each input, create CommitSource or PRSource
   - PR number → PRSource (uses merge_commit_sha for merged PRs)
   - Commit SHA → CommitSource (always uses commit.sha)
3. Validate: Check PRs are merged, commits/branches exist
4. For each target branch:
   a. Clone repository
   b. Fetch and checkout target branch
   c. Create dev branch
   d. Cherry-pick each commit SHA in order (from source_info.commit_shas)
   e. Handle conflicts (commit with BACKPORT-CONFLICT message)
   f. Push branch
   g. Create PR (draft if conflicts, normal otherwise)
   h. Update comments on original PRs

Key Design Decisions:
--------------------

- Order preservation: Sources stored in single list (SourceInfo.sources)
- Merged PR handling: Always uses merge_commit_sha (same as old script)
- Empty commits: Handled via --allow-empty flag (same as old script)
- Conflict handling: Commits conflicts for manual resolution
- PR metadata: Extracts changelog category/entry from original PRs
"""

import os
import sys
import datetime
import logging
import subprocess
import argparse
import re
from typing import List, Optional, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from github import Github, GithubException, PullRequest
import requests
import git

# Import PR template and categories from validate_pr_description action
try:
    pr_template_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'actions', 'validate_pr_description'))
    sys.path.insert(0, pr_template_path)
    from pr_template import (
        ISSUE_PATTERNS,
        get_category_section_template
    )
except ImportError as e:
    logging.error(f"Failed to import pr_template: {e}")
    raise


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class ConflictInfo:
    """Information about a conflict in a file"""
    file_path: str
    line: Optional[int] = None
    message: Optional[str] = None


@dataclass
class CherryPickResult:
    """Result of git cherry-pick execution"""
    success: bool
    raw_output: str = ""  # Raw output of git command
    conflict_files: List[ConflictInfo] = field(default_factory=list)
    
    @property
    def has_conflicts(self) -> bool:
        """Check if there are conflicts based on conflict_files or raw_output"""
        if self.conflict_files:
            return True
        # Fallback to checking raw_output if conflict_files not yet populated
        return "conflict" in self.raw_output.lower() if self.raw_output else False


class AbstractCommit(ABC):
    """Abstract base class for commit sources (commits and PRs)"""
    
    @abstractmethod
    def get_commit_shas(self) -> List[str]:
        """Returns list of commit SHAs to cherry-pick"""
        pass
    
    @abstractmethod
    def get_title(self, single: bool) -> str:
        """Returns title for display"""
        pass
    
    @abstractmethod
    def get_body_item(self) -> str:
        """Returns body item for PR description"""
        pass
    
    @abstractmethod
    def get_author(self) -> Optional[str]:
        """Returns author username"""
        pass
    
    @abstractmethod
    def get_pull_requests(self) -> List[PullRequest]:
        """Returns list of associated PullRequest objects"""
        pass


class CommitSource(AbstractCommit):
    """Source representing a single commit"""
    
    def __init__(self, commit, repo, merge_commits_mode: str = 'skip'):
        self.commit = commit
        self.repo = repo
        self.merge_commits_mode = merge_commits_mode
        self._linked_pr: Optional[PullRequest] = None
        # Cache linked PR if available
        try:
            pulls = commit.get_pulls()
            if pulls.totalCount > 0:
                self._linked_pr = pulls.get_page(0)[0]
        except Exception:
            pass
    
    def get_commit_shas(self) -> List[str]:
        return [self.commit.sha]
    
    def get_title(self, single: bool) -> str:
        if single:
            return f'cherry-pick commit {self.commit.sha[:7]}'
        return f'commit {self.commit.sha[:7]}'
    
    def get_body_item(self) -> str:
        if self._linked_pr:
            return f"* commit {self.commit.html_url}: {self._linked_pr.title}"
        return f"* commit {self.commit.html_url}"
    
    def get_author(self) -> Optional[str]:
        # Prefer PR author if commit is linked to PR
        if self._linked_pr:
            return self._linked_pr.user.login
        try:
            return self.commit.author.login if self.commit.author else None
        except AttributeError:
            return None
    
    def get_pull_requests(self) -> List[PullRequest]:
        if self._linked_pr:
            return [self._linked_pr]
        return []


class PRSource(AbstractCommit):
    """Source representing a Pull Request"""
    
    def __init__(self, pull, repo, merge_commits_mode: str = 'skip', allow_unmerged: bool = False):
        self.pull = pull
        self.repo = repo
        self.merge_commits_mode = merge_commits_mode
        self.allow_unmerged = allow_unmerged
        self._commit_shas: Optional[List[str]] = None
    
    def _get_commits_from_pr(self) -> List[str]:
        """Gets list of commits from PR (excluding merge commits)"""
        commits = []
        for commit in self.pull.get_commits():
            commit_obj = commit.commit
            if self.merge_commits_mode == 'skip':
                if commit_obj.parents and len(commit_obj.parents) > 1:
                    continue
            commits.append(commit.sha)
        return commits
    
    def get_commit_shas(self) -> List[str]:
        """Returns commit SHAs to cherry-pick"""
        if self._commit_shas is not None:
            return self._commit_shas
        
        if not self.pull.merged:
            # Unmerged PR - use all commits from PR
            pr_commits = self._get_commits_from_pr()
            if not pr_commits:
                raise ValueError(f"PR #{self.pull.number} contains no commits to cherry-pick")
            self._commit_shas = pr_commits
        elif self.pull.merge_commit_sha:
            # Merged PR - always use merge_commit_sha (same as old script)
            self._commit_shas = [self.pull.merge_commit_sha]
        else:
            # Fallback: use all commits from PR if no merge_commit_sha
            pr_commits = self._get_commits_from_pr()
            if not pr_commits:
                raise ValueError(f"PR #{self.pull.number} contains no commits to cherry-pick")
            self._commit_shas = pr_commits
        
        return self._commit_shas
    
    def get_title(self, single: bool) -> str:
        if single:
            return self.pull.title
        return f'PR {self.pull.number}'
    
    def get_body_item(self) -> str:
        return f"* PR {self.pull.html_url}"
    
    def get_author(self) -> Optional[str]:
        return self.pull.user.login
    
    def get_pull_requests(self) -> List[PullRequest]:
        return [self.pull]


@dataclass
class SourceInfo:
    """Information about source PRs/commits for backport"""
    sources: List[AbstractCommit] = field(default_factory=list)
    
    @property
    def commit_shas(self) -> List[str]:
        """Get all commit SHAs in order"""
        result = []
        for source in self.sources:
            result.extend(source.get_commit_shas())
        return result
    
    @property
    def pull_requests(self) -> List[PullRequest]:
        """Get all pull requests in order"""
        result = []
        for source in self.sources:
            result.extend(source.get_pull_requests())
        return result
    
    @property
    def titles(self) -> List[str]:
        """Get all titles in order"""
        single = len(self.sources) == 1
        return [source.get_title(single) for source in self.sources]
    
    @property
    def body_items(self) -> List[str]:
        """Get all body items in order"""
        return [source.get_body_item() for source in self.sources]
    
    @property
    def authors(self) -> List[str]:
        """Get all authors in order"""
        result = []
        for source in self.sources:
            author = source.get_author()
            if author and author not in result:
                result.append(author)
        return result


@dataclass
class PRContext:
    """Context for PR body generation"""
    target_branch: str
    dev_branch_name: str
    repo_name: str
    pr_number: Optional[int] = None
    
    # Source info - single source of truth
    source_info: SourceInfo
    
    # Changelog (computed in build_body)
    changelog_category: Optional[str] = None
    changelog_entry: str = ""
    
    # Conflicts
    has_conflicts: bool = False
    conflict_files: List[ConflictInfo] = field(default_factory=list)
    
    # Logs
    cherry_pick_logs: List[str] = field(default_factory=list)  # Raw git cherry-pick logs
    
    # Workflow
    workflow_url: Optional[str] = None
    workflow_triggerer: str = "unknown"


@dataclass
class BackportResult:
    """Backport result for a single branch"""
    target_branch: str
    pr: Optional[PullRequest] = None
    conflict_files: List[ConflictInfo] = field(default_factory=list)
    error: Optional[str] = None
    cherry_pick_logs: List[str] = field(default_factory=list)
    
    @property
    def has_conflicts(self) -> bool:
        """Check if there are conflicts based on conflict_files"""
        return len(self.conflict_files) > 0


# ============================================================================
# Core Classes
# ============================================================================

class CommitResolver:
    """Resolves and expands commit references (SHA, short SHA, PR number)"""
    
    FULL_SHA_LENGTH = 40
    
    def __init__(self, repo, logger):
        self.repo = repo
        self.logger = logger
    
    def expand_sha(self, ref: str) -> str:
        """Expands short SHA to full SHA using GitHub API"""
        if len(ref) == self.FULL_SHA_LENGTH:
            # Already full SHA, verify it exists
            try:
                commits = self.repo.get_commits(sha=ref)
                if commits.totalCount > 0 and commits[0].sha == ref:
                    return ref
            except Exception as e:
                self.logger.debug(f"Failed to verify full SHA via GitHub API: {e}")
        else:
            # Short SHA, expand it
            try:
                commits = self.repo.get_commits(sha=ref)
                if commits.totalCount > 0:
                    commit = commits[0]
                    if commit.sha.startswith(ref):
                        return commit.sha
            except Exception as e:
                self.logger.debug(f"Failed to find commit via GitHub API: {e}")
        
        raise ValueError(f"Failed to find commit for '{ref}'")


class GitRepository:
    """Abstraction over git commands using GitPython"""
    
    def __init__(self, logger, repo_path: Optional[str] = None):
        self.logger = logger
        self.repo: Optional[git.Repo] = None
        if repo_path:
            self.repo = git.Repo(repo_path)
    
    def clone(self, repo_url: str, target_dir: str):
        """Clones repository"""
        self.logger.info("Cloning repository: %s to %s", repo_url, target_dir)
        try:
            self.repo = git.Repo.clone_from(
                repo_url,
                target_dir,
                env={'GIT_PROTOCOL': '2'}
            )
            self.logger.debug("Repository cloned successfully")
        except git.GitCommandError as e:
            self.logger.error(f"Failed to clone repository: {e}")
            raise
    
    def fetch(self, remote: str, branch: str):
        """Fetches branch"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Fetching %s/%s", remote, branch)
        try:
            self.repo.remote(remote).fetch(branch)
            self.logger.debug("Branch fetched successfully")
        except git.GitCommandError as e:
            self.logger.error(f"Failed to fetch branch: {e}")
            raise
    
    def reset_hard(self):
        """Hard reset current branch"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Hard reset current branch")
        try:
            self.repo.head.reset(index=True, working_tree=True)
            self.logger.debug("Hard reset completed")
        except git.GitCommandError as e:
            self.logger.error(f"Failed to reset: {e}")
            raise
    
    def checkout_branch(self, branch: str, from_branch: Optional[str] = None):
        """Checkout branch (creates if doesn't exist)"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Checkout branch: %s", branch)
        try:
            if from_branch:
                # from_branch can be "origin/branch" format
                # Use git command for simplicity with remote refs
                self.repo.git.checkout("-B", branch, from_branch)
            else:
                self.repo.git.checkout(branch)
            self.logger.debug("Branch checked out successfully")
        except git.GitCommandError as e:
            self.logger.error(f"Failed to checkout branch: {e}")
            raise
    
    def create_branch(self, branch_name: str, from_branch: str):
        """Creates new branch from specified branch"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Create branch: %s from %s", branch_name, from_branch)
        try:
            # from_branch can be "origin/branch" format
            # Use git command for simplicity with remote refs
            self.repo.git.checkout("-b", branch_name, from_branch)
            self.logger.debug("Branch created and checked out successfully")
            return branch_name
        except git.GitCommandError as e:
            self.logger.error(f"Failed to create branch: {e}")
            raise
    
    def cherry_pick(self, commit_sha: str) -> CherryPickResult:
        """Cherry-pick commit with full output preservation"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Cherry-picking commit: %s", commit_sha[:7])
        # Use subprocess to preserve full raw output for logging
        try:
            result = subprocess.run(
                ['git', 'cherry-pick', '--allow-empty', commit_sha],
                cwd=self.repo.working_dir,
                capture_output=True,
                text=True,
                check=True
            )
            output = (result.stdout or '') + (('\n' + result.stderr) if result.stderr else '')
            return CherryPickResult(
                success=True,
                raw_output=output
            )
        except subprocess.CalledProcessError as e:
            output = (e.stdout or '') + (('\n' + e.stderr) if e.stderr else '')
            output_lower = output.lower()
            
            has_conflicts = "conflict" in output_lower
            
            if has_conflicts:
                return CherryPickResult(
                    success=False,
                    raw_output=output
                )
            
            # Other error - re-raise
            self.logger.error(f"Cherry-pick failed: {e}")
            raise
    
    def add_all(self):
        """git add -A"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Adding all files")
        try:
            self.repo.git.add(A=True)
            self.logger.debug("Files added successfully")
        except git.GitCommandError as e:
            self.logger.error(f"Failed to add files: {e}")
            raise
    
    def commit(self, message: str):
        """git commit"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Committing: %s", message[:50])
        try:
            self.repo.index.commit(message)
            self.logger.debug("Commit created successfully")
        except git.GitCommandError as e:
            self.logger.error(f"Failed to commit: {e}")
            raise
    
    def push(self, branch: str, set_upstream: bool = True):
        """Push branch"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Pushing branch: %s", branch)
        try:
            if set_upstream:
                self.repo.git.push("--set-upstream", "origin", branch)
            else:
                self.repo.remote("origin").push(branch)
            self.logger.debug("Branch pushed successfully")
        except git.GitCommandError as e:
            self.logger.error(f"Failed to push branch: {e}")
            raise
    
    def cherry_pick_abort(self):
        """Abort cherry-pick"""
        if not self.repo:
            raise RuntimeError("Repository not initialized. Call clone() first.")
        self.logger.info("Aborting cherry-pick")
        try:
            self.repo.git.cherry_pick("--abort")
            self.logger.debug("Cherry-pick aborted successfully")
        except git.GitCommandError:
            # May not be in cherry-pick state
            self.logger.debug("No cherry-pick in progress to abort")
            pass


class ConflictHandler:
    """Handles conflicts during cherry-pick"""
    
    CONFLICT_STATUS_CODES = ['UU', 'AA', 'DD', 'DU', 'UD', 'AU', 'UA']
    
    def __init__(self, git_repo: GitRepository, logger):
        self.git_repo = git_repo
        self.logger = logger
    
    def extract_conflict_messages(self, git_output: str) -> List[str]:
        """Extracts CONFLICT messages from git output"""
        conflict_messages = []
        lines = git_output.split('\n')
        
        for i, line in enumerate(lines):
            if 'CONFLICT' in line and '(' in line and ')' in line:
                conflict_msg = line.strip()
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    if 'CONFLICT' in next_line and '(' in next_line and ')' in next_line:
                        break
                    if next_line.startswith('hint:') or next_line.startswith('error:') or not next_line:
                        break
                    if next_line:
                        conflict_msg += ' ' + next_line
                    j += 1
                conflict_messages.append(conflict_msg)
        
        return conflict_messages
    
    def find_conflict_message_for_file(self, file_path: str, conflict_messages: List[str]) -> Optional[str]:
        """Finds conflict message for a file"""
        file_basename = os.path.basename(file_path)
        for msg in conflict_messages:
            if ': ' in msg:
                msg_file_part = msg.split(': ', 1)[1]
                msg_filename = msg_file_part.split()[0] if msg_file_part.split() else ""
                if msg_filename == file_path or msg_filename == file_basename:
                    return msg
            elif file_path in msg:
                if re.search(rf'\b{re.escape(file_path)}\b', msg) or re.search(rf'\b{re.escape(file_basename)}\b', msg):
                    return msg
        return None
    
    def find_first_conflict_line(self, file_path: str) -> Optional[int]:
        """Finds first line with conflict"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, start=1):
                    if line.strip().startswith('<<<<<<<'):
                        return line_num
        except (FileNotFoundError, IOError, UnicodeDecodeError) as e:
            self.logger.debug(f"Failed to read file {file_path} to find conflict line: {e}")
        return None
    
    def detect_conflicts(self, git_output: str) -> List[ConflictInfo]:
        """Detects conflicts from git status and output"""
        conflict_files = []
        conflict_messages = self.extract_conflict_messages(git_output)
        
        try:
            result = subprocess.run(
                ['git', 'status', '--porcelain'],
                capture_output=True,
                text=True,
                check=True
            )
            
            if result.stdout.strip():
                status_lines = result.stdout.strip().split('\n')
                self.logger.debug(f"Git status output: {result.stdout.strip()}")
                
                for line in status_lines:
                    status_code = line[:2]
                    if status_code in self.CONFLICT_STATUS_CODES:
                        parts = line.split(None, 1)
                        if len(parts) > 1:
                            file_path = parts[1].strip()
                            if file_path:
                                conflict_line = self.find_first_conflict_line(file_path)
                                conflict_message = self.find_conflict_message_for_file(file_path, conflict_messages)
                                conflict_files.append(ConflictInfo(
                                    file_path=file_path,
                                    line=conflict_line,
                                    message=conflict_message
                                ))
                
                # If not found via status, try git diff
                if not conflict_files:
                    try:
                        diff_result = subprocess.run(
                            ['git', 'diff', '--name-only', '--diff-filter=U'],
                            capture_output=True,
                            text=True,
                            check=True
                        )
                        if diff_result.stdout.strip():
                            for f in diff_result.stdout.strip().split('\n'):
                                file_path = f.strip()
                                if file_path:
                                    conflict_line = self.find_first_conflict_line(file_path)
                                    conflict_message = self.find_conflict_message_for_file(file_path, conflict_messages)
                                    conflict_files.append(ConflictInfo(
                                        file_path=file_path,
                                        line=conflict_line,
                                        message=conflict_message
                                    ))
                    except subprocess.CalledProcessError:
                        pass
                
                if conflict_files:
                    self.logger.info(f"Detected {len(conflict_files)} conflict(s):")
                    for conflict in conflict_files:
                        if conflict.message:
                            self.logger.info(f"  {conflict.message}")
                        else:
                            self.logger.info(f"  - {conflict.file_path}" + (f" (line {conflict.line})" if conflict.line else ""))
        
        except Exception as e:
            self.logger.error(f"Error detecting conflicts: {e}")
        
        return conflict_files
    
    def commit_conflicts(self, commit_sha: str, conflict_files: List[ConflictInfo]) -> bool:
        """Commits conflicts for manual resolution"""
        try:
            self.git_repo.add_all()
            self.git_repo.commit(f"BACKPORT-CONFLICT: manual resolution required for commit {commit_sha[:7]}")
            self.logger.info(f"Conflict committed for commit {commit_sha[:7]}")
            return True
        except Exception as e:
            self.logger.error(f"Error committing conflicts for commit {commit_sha[:7]}: {e}")
            return False


class GitHubClient:
    """Wrapper over GitHub API"""
    
    def __init__(self, repo, token, logger):
        self.repo = repo
        self.token = token
        self.logger = logger
    
    def get_linked_issues_graphql(self, pr_number: int) -> List[str]:
        """Gets linked issues via GraphQL API"""
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
        
        owner, repo_name = self.repo.full_name.split('/')
        variables = {
            "owner": owner,
            "repo": repo_name,
            "prNumber": pr_number
        }
        
        try:
            response = requests.post(
                "https://api.github.com/graphql",
                headers={"Authorization": f"token {self.token}"},
                json={"query": query, "variables": variables},
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            if 'errors' in data:
                self.logger.warning(f"GraphQL errors for PR #{pr_number}: {data['errors']}")
                return []
            
            issues = []
            nodes = data.get("data", {}).get("repository", {}).get("pullRequest", {}).get("closingIssuesReferences", {}).get("nodes", [])
            for issue in nodes:
                owner_name = issue["repository"]["owner"]["login"]
                repo_name_issue = issue["repository"]["name"]
                number = issue["number"]
                if owner_name == owner and repo_name_issue == repo_name:
                    issues.append(f"#{number}")
                else:
                    issues.append(f"{owner_name}/{repo_name_issue}#{number}")
            
            return issues
        except Exception as e:
            self.logger.warning(f"Failed to get linked issues via GraphQL for PR #{pr_number}: {e}")
            return []

    def get_linked_issues(self, pull_requests: List) -> str:
        """Gets linked issues for all PRs"""
        all_issues = []
        
        for pull in pull_requests:
            issues = self.get_linked_issues_graphql(pull.number)
            
            # If GraphQL didn't return issues, parse from PR body
            if not issues and pull.body:
                body_issues = re.findall(r'#(\d+)', pull.body)
                issues = [f"#{num}" for num in body_issues]
            
            all_issues.extend(issues)
        
        # Remove duplicates
        unique_issues = list(dict.fromkeys(all_issues))
        return ' '.join(unique_issues) if unique_issues else 'None'

    def create_pr(self, base: str, head: str, title: str, body: str, draft: bool = False):
        """Creates PR"""
        return self.repo.create_pull(
            base=base,
            head=head,
            title=title,
            body=body,
            maintainer_can_modify=True,
            draft=draft
        )


class PRContentBuilder:
    """Generates PR content (title, body)"""
    
    def __init__(self, repo_name: str, github_client: GitHubClient, logger):
        self.repo_name = repo_name
        self.github_client = github_client
        self.logger = logger
    
    def extract_changelog_category(self, pr_body: str) -> Optional[str]:
        """Extracts Changelog category from PR body"""
        if not pr_body:
            return None
        
        category_match = re.search(r"### Changelog category.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
        if not category_match:
            return None
        
        categories = [line.strip('* ').strip() for line in category_match.group(1).splitlines() if line.strip() and line.strip().startswith('*')]
        
        for cat in categories:
            cat_clean = cat.strip()
            if cat_clean:
                return cat_clean
        
        return None

    def extract_changelog_entry(self, pr_body: str, stop_at_category: bool = False) -> Optional[str]:
        """Extracts Changelog entry from PR body"""
        if not pr_body:
            return None
        
        if stop_at_category:
            pattern = r"### Changelog entry.*?\n(.*?)(\n### Changelog category|$)"
        else:
            pattern = r"### Changelog entry.*?\n(.*?)(\n###|$)"
        
        entry_match = re.search(pattern, pr_body, re.DOTALL)
        if not entry_match:
            return None
        
        entry = entry_match.group(1).strip()
        if entry in ['...', '']:
            return None
        
        return entry

    def extract_changelog_entry_content(self, pr_body: str) -> Optional[str]:
        """Extracts only Changelog entry content"""
        return self.extract_changelog_entry(pr_body, stop_at_category=True)
    
    def _format_items_list(self, items: List, item_type: str, format_func) -> str:
        """Formats list of items into string with correct grammar
        
        Args:
            items: List of items
            item_type: Item type in singular (e.g., "PR", "commit")
            format_func: Function to format single item (e.g., lambda p: f"#{p.number}")
        
        Returns:
            Formatted string: "PR #123" or "PRs #123, #456"
        """
        if not items:
            return ""
        if len(items) == 1:
            return f"{item_type} {format_func(items[0])}"
        else:
            formatted = ', '.join([format_func(item) for item in items])
            return f"{item_type}s {formatted}"
    
    def get_changelog_info(self, pull_requests: List) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Gets Changelog information from all PRs"""
        categories = []
        entry = None
        entry_contents = []
        
        for pull in pull_requests:
            if pull.body:
                cat = self.extract_changelog_category(pull.body)
                if cat:
                    categories.append(cat)
                
                if not entry:
                    ent = self.extract_changelog_entry(pull.body)
                    if ent:
                        entry = ent
                
                entry_content = self.extract_changelog_entry_content(pull.body)
                if entry_content:
                    entry_contents.append(entry_content)
        
        category = None
        if categories:
            unique_categories = set(categories)
            if len(unique_categories) == 1:
                category = categories[0]
        
        if len(entry_contents) > 1:
            merged_entry_content = "\n\n---\n\n".join(entry_contents)
        elif len(entry_contents) == 1:
            merged_entry_content = entry_contents[0]
        else:
            merged_entry_content = None
        
        return category, entry, merged_entry_content

    def build_title(self, target_branch: str, source_info: SourceInfo, has_conflicts: bool = False) -> str:
        """Generates title with CONFLICT if conflicts exist"""
        if len(source_info.titles) == 1:
            base_title = f"[Backport {target_branch}] {source_info.titles[0]}"
        else:
            base_title = f"[Backport {target_branch}] cherry-pick {', '.join(source_info.titles)}"
        
        # Add CONFLICT prefix if conflicts exist
        if has_conflicts:
            base_title = f"[CONFLICT] {base_title}"
        
        # Truncate long titles
        if len(base_title) > 200:
            base_title = base_title[:197] + "..."
        
        return base_title
    
    def build_body(self, context: PRContext) -> str:
        """Generates PR body with raw cherry-pick log"""
        # Get issues
        issue_refs = self.github_client.get_linked_issues(context.source_info.pull_requests)
        
        # Format authors
        authors = ', '.join([f"@{author}" for author in set(context.source_info.authors)]) if context.source_info.authors else "Unknown"
        
        # Get changelog information
        changelog_category, changelog_entry_text, merged_entry_content = self.get_changelog_info(context.source_info.pull_requests)
        context.changelog_category = changelog_category
        
        # Determine changelog entry
        branch_desc = context.target_branch
        if merged_entry_content:
            changelog_entry = merged_entry_content
        elif not changelog_entry_text:
            # Use PR or commits
            if context.source_info.pull_requests:
                items_str = self._format_items_list(
                    context.source_info.pull_requests,
                    "PR",
                    lambda p: f"#{p.number}"
                )
            else:
                items_str = self._format_items_list(
                    context.source_info.commit_shas,
                    "commit",
                    lambda sha: sha[:7]
                )
            changelog_entry = f"Backport of {items_str} to `{branch_desc}`"
        else:
            changelog_entry = changelog_entry_text
        
        # Ensure entry is at least 20 characters
        if len(changelog_entry) < 20:
            changelog_entry = f"Backport to `{branch_desc}`: {changelog_entry}"
        
        # For Bugfix category add issue reference
        if changelog_category and changelog_category == "Bugfix" and issue_refs and issue_refs != "None":
            has_issue = any(re.search(pattern, changelog_entry) for pattern in ISSUE_PATTERNS)
            if not has_issue:
                changelog_entry = f"{changelog_entry} ({issue_refs})"
        
        context.changelog_entry = changelog_entry
        
        # Build category section
        if changelog_category:
            category_section = f"* {changelog_category}"
        else:
            category_section = get_category_section_template()
        
        # Build description section
        commits = '\n'.join([f"* {item}" for item in context.source_info.body_items])
        
        description_section = f"#### Original PR(s)\n{commits}\n\n"
        description_section += f"#### Metadata\n"
        description_section += f"- **Original PR author(s):** {authors}\n"
        description_section += f"- **Cherry-picked by:** @{context.workflow_triggerer}\n"
        description_section += f"- **Related issues:** {issue_refs}"
        
        # Add raw git cherry-pick log (if exists)
        cherry_pick_log_section = ""
        if context.cherry_pick_logs:
            cherry_pick_log_section = "\n\n### Git Cherry-Pick Log\n\n"
            cherry_pick_log_section += "```\n"
            for log in context.cherry_pick_logs:
                cherry_pick_log_section += log
                if not log.endswith('\n'):
                    cherry_pick_log_section += '\n'
            cherry_pick_log_section += "```\n"
        
        # Add conflicts section (if exists)
        conflicts_section = ""
        if context.has_conflicts:
            branch_for_instructions = context.dev_branch_name or context.target_branch
            conflicts_section = "\n\n#### Conflicts Require Manual Resolution\n\n"
            conflicts_section += "This PR contains merge conflicts that require manual resolution.\n\n"
            
            if context.conflict_files:
                conflicts_section += "**Files with conflicts:**\n\n"
                for conflict in context.conflict_files:
                    # Simple link to files in PR (without diff hash)
                    if context.pr_number:
                        file_link = f"https://github.com/{self.repo_name}/pull/{context.pr_number}/files"
                    else:
                        file_link = f"https://github.com/{self.repo_name}/blob/{branch_for_instructions}/{conflict.file_path}"
                    conflicts_section += f"- [{conflict.file_path}]({file_link})\n"
                    if conflict.message:
                        conflicts_section += f"  - `{conflict.message}`\n"
            
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
        
        # Add workflow link
        workflow_section = ""
        if context.workflow_url:
            workflow_section = f"\n\n---\n\nPR was created by cherry-pick workflow [run]({context.workflow_url})"
        else:
            workflow_section = "\n\n---\n\nPR was created by cherry-pick script"
        
        # Assemble full body
        pr_body = f"""### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

{changelog_entry}

### Changelog category <!-- remove all except one -->

{category_section}

### Description for reviewers <!-- (optional) description for those who read this PR -->

{description_section}{cherry_pick_log_section}{conflicts_section}{workflow_section}
"""
        
        return pr_body
    

class CommentManager:
    """Manages comments in original PRs"""
    
    def __init__(self, logger):
        self.logger = logger
        self.backport_comments = []  # [(pull, comment), ...]
    
    def find_existing_backport_comment(self, pull):
        """Finds existing backport comment"""
        try:
            comments = pull.get_issue_comments()
            for comment in comments:
                if comment.user.login == "YDBot" and "Backport" in comment.body:
                    if "in progress" in comment.body:
                        return comment
        except Exception as e:
            self.logger.debug(f"Failed to find existing comment in PR #{pull.number}: {e}")
        return None

    def create_initial_comment(self, pull_requests: List, target_branches: List[str], workflow_url: Optional[str]):
        """Creates or updates initial comment about backport start"""
        if not workflow_url:
            self.logger.warning("Workflow URL not available, skipping initial comment")
            return
        
        target_branches_str = ', '.join([f"`{b}`" for b in target_branches])
        new_line = f"Backport to {target_branches_str} in progress: [workflow run]({workflow_url})"
        
        for pull in pull_requests:
            try:
                existing_comment = self.find_existing_backport_comment(pull)
                if existing_comment:
                    existing_body = existing_comment.body
                    branches_already_mentioned = all(f"`{b}`" in existing_body for b in target_branches)
                    
                    if branches_already_mentioned and workflow_url in existing_body:
                        self.backport_comments.append((pull, existing_comment))
                        self.logger.debug(f"Backport comment already contains info for branches {target_branches}, skipping update")
                    else:
                        updated_comment = f"{existing_body}\n\n{new_line}"
                        existing_comment.edit(updated_comment)
                        self.backport_comments.append((pull, existing_comment))
                        self.logger.info(f"Updated existing backport comment in original PR #{pull.number}")
                else:
                    comment = pull.create_issue_comment(new_line)
                    self.backport_comments.append((pull, comment))
                    self.logger.info(f"Created initial backport comment in original PR #{pull.number}")
            except GithubException as e:
                self.logger.warning(f"Failed to create/update initial comment in original PR #{pull.number}: {e}")

    def update_with_results(self, results: List[BackportResult], skipped_branches: List[Tuple[str, str]], target_branches: List[str], workflow_url: Optional[str]):
        """Updates comments with backport results"""
        if not self.backport_comments:
            return
        
        for pull, comment in self.backport_comments:
            try:
                existing_body = comment.body
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
                
                # Replace "in progress" line
                if workflow_url in existing_body:
                    lines = existing_body.split('\n')
                    updated_lines = []
                    workflow_found = False
                    in_results_block = False
                    
                    for line in lines:
                        if workflow_url in line:
                            if "in progress" in line:
                                updated_lines.append(new_results)
                                workflow_found = True
                                if new_results.count('\n') > 0:
                                    in_results_block = True
                            elif in_results_block:
                                if line.strip() == '':
                                    in_results_block = False
                                    updated_lines.append(line)
                                elif workflow_url not in line:
                                    in_results_block = False
                                    updated_lines.append(line)
                            else:
                                is_result_line = (
                                    any(f"`{b}`" in line for b in target_branches) or
                                    "Backport results:" in line or
                                    line.strip().startswith("- `")
                                )
                                if is_result_line and workflow_url in line:
                                    updated_lines.append(new_results)
                                    workflow_found = True
                                    if new_results.count('\n') > 0:
                                        in_results_block = True
                                else:
                                    updated_lines.append(line)
                        elif in_results_block:
                            if line.strip() == '':
                                in_results_block = False
                                updated_lines.append(line)
                        else:
                            updated_lines.append(line)
                    
                    if not workflow_found:
                        updated_lines.append("")
                        updated_lines.append(new_results)
                    
                    updated_comment = '\n'.join(updated_lines)
                else:
                    updated_comment = f"{existing_body}\n\n{new_results}"
                
                comment.edit(updated_comment)
                self.logger.info(f"Updated backport comment in original PR #{pull.number}")
            except GithubException as e:
                self.logger.warning(f"Failed to update comment in original PR #{pull.number}: {e}")


class SummaryWriter:
    """Writes to GitHub Actions summary"""
    
    def __init__(self, logger):
        self.logger = logger
        self.summary_path = os.getenv('GITHUB_STEP_SUMMARY')
    
    def write(self, msg: str):
        """Writes message to summary and logs"""
        self.logger.info(msg)
        if self.summary_path:
            with open(self.summary_path, 'a') as summary:
                summary.write(f'{msg}\n\n')
    
    def write_branch_result(self, branch: str, pr, has_conflicts: bool, conflict_files: List[ConflictInfo], cherry_pick_logs: List[str]):
        """Writes branch result with raw log"""
        summary = f"### Branch `{branch}`: "
        if has_conflicts:
            summary += f"**CONFLICT** Draft PR {pr.html_url}\n\n"
        else:
            summary += f"PR {pr.html_url}\n\n"
        
        # Add raw log
        if cherry_pick_logs:
            summary += "**Git Cherry-Pick Log:**\n\n"
            summary += "```\n"
            for log in cherry_pick_logs:
                summary += log
            summary += "```\n\n"
        
        # Add conflicts (if exists)
        if has_conflicts and conflict_files:
            summary += "**Files with conflicts:**\n\n"
            for conflict in conflict_files:
                summary += f"- `{conflict.file_path}`\n"
                if conflict.message:
                    summary += f"  - `{conflict.message}`\n"
        
        self.write(summary)


class InputParser:
    """Parses and normalizes input data"""
    
    @staticmethod
    def split(s: str, seps: str = ', \n') -> List[str]:
        """Splits string by multiple separators (any of the characters in seps) and filters out empty strings"""
        if not s:
            return []
        if not seps:
            return [s] if s.strip() else []
        # Build regex pattern to match any separator character
        pattern = f"[{re.escape(seps)}]+"
        return [part.strip() for part in re.split(pattern, s) if part.strip()]
    
    @staticmethod
    def normalize_commit_ref(ref: str) -> str:
        """Normalizes commit reference (removes URL, leaves only ID)"""
        return ref.split('/')[-1].strip()


class InputValidator:
    """Validates input data"""
    
    def __init__(self, repo, allow_unmerged: bool, logger):
        self.repo = repo
        self.allow_unmerged = allow_unmerged
        self.logger = logger
    
    def validate_prs(self, pull_requests: List):
        """Validates PRs"""
        for pull in pull_requests:
            try:
                if not hasattr(pull, "merged"):
                    raise ValueError(f"PR #{pull.number} object is missing 'merged' attribute, cannot validate")
                if not pull.merged and not self.allow_unmerged:
                    raise ValueError(f"PR #{pull.number} is not merged. Use --allow-unmerged to allow backporting unmerged PRs")
            except GithubException as e:
                raise ValueError(f"PR #{pull.number} does not exist or is not accessible: {e}")
    
    def validate_commits(self, commit_shas: List[str]):
        """Validates commits"""
        for commit_sha in commit_shas:
            try:
                commit = self.repo.get_commit(commit_sha)
                if not commit.sha:
                    raise ValueError(f"Commit {commit_sha} not found")
            except GithubException as e:
                raise ValueError(f"Commit {commit_sha} does not exist: {e}")
    
    def validate_branches(self, branches: List[str]):
        """Validates branches"""
        for branch in branches:
            try:
                self.repo.get_branch(branch)
            except GithubException as e:
                raise ValueError(f"Branch {branch} does not exist: {e}")
    
    def validate_all(self, commit_shas: List[str], branches: List[str], pull_requests: List):
        """Validates all input data"""
        if len(commit_shas) == 0:
            raise ValueError("No commits to cherry-pick")
        if len(branches) == 0:
            raise ValueError("No target branches specified")
        
        self.logger.info("Validating input data...")
        self.validate_prs(pull_requests)
        self.validate_commits(commit_shas)
        self.validate_branches(branches)
        self.logger.info("Input validation successful")


class CherryPickOrchestrator:
    """Main class for coordinating cherry-pick process with new architecture"""
    
    def __init__(self, args, repo_name: str, token: str, workflow_triggerer: str, workflow_url: Optional[str] = None):
        self.logger = logging.getLogger("cherry-pick")
        self.repo_name = repo_name
        self.token = token
        self.workflow_triggerer = workflow_triggerer
        self.workflow_url = workflow_url
        
        # Initialize components
        self.gh = Github(login_or_token=token)
        self.repo = self.gh.get_repo(repo_name)
        
        self.commit_resolver = CommitResolver(self.repo, self.logger)
        self.git_repo = GitRepository(self.logger)
        self.github_client = GitHubClient(self.repo, token, self.logger)
        self.pr_builder = PRContentBuilder(repo_name, self.github_client, self.logger)
        self.conflict_handler = ConflictHandler(self.git_repo, self.logger)
        self.comment_manager = CommentManager(self.logger)
        self.summary_writer = SummaryWriter(self.logger)
        self.validator = InputValidator(self.repo, getattr(args, 'allow_unmerged', False), self.logger)
        
        # Parameters
        self.merge_commits_mode = getattr(args, 'merge_commits', 'skip')
        self.allow_unmerged = getattr(args, 'allow_unmerged', False)
        
        # Parse input data
        commits_str = args.commits
        branches_str = args.target_branches
        
        commits = InputParser.split(commits_str)
        self.target_branches = InputParser.split(branches_str)
        
        # Collect information about source PRs/commits
        self.source_info = SourceInfo()
        
        for c in commits:
            ref = InputParser.normalize_commit_ref(c)
            try:
                pr_num = int(ref)
                self._add_pull(pr_num, len(commits) == 1)
            except ValueError:
                self._add_commit(ref, len(commits) == 1)
        
        # Results
        self.results: List[BackportResult] = []
        self.skipped_branches: List[Tuple[str, str]] = []
        self.has_errors = False
        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M%S")
    
    def _add_commit(self, c: str, single: bool):
        """Adds commit by SHA"""
        try:
            expanded_sha = self.commit_resolver.expand_sha(c)
        except ValueError as e:
            self.logger.error(f"Failed to expand SHA {c}: {e}")
            raise
        
        commit = self.repo.get_commit(expanded_sha)
        
        # Check merge commit
        is_merge_commit = commit.parents and len(commit.parents) > 1
        
        # First check if commit is linked to PR (even if it's a merge commit)
        pulls = commit.get_pulls()
        if pulls.totalCount > 0:
            pr = pulls.get_page(0)[0]
            
            # If this is merge commit linked to PR - automatically use PR
            if is_merge_commit:
                if self.merge_commits_mode == 'fail':
                    self.logger.warning(
                        f"Commit {expanded_sha[:7]} is a merge commit associated with PR #{pr.number}. "
                        f"Automatically using PR #{pr.number} instead."
                    )
                elif self.merge_commits_mode == 'skip':
                    self.logger.info(
                        f"Commit {expanded_sha[:7]} is a merge commit associated with PR #{pr.number}. "
                        f"Automatically using PR #{pr.number} instead of skipping."
                    )
                # Process as PR instead of merge commit
                self._add_pull(pr.number, single)
                return
            
            # Regular commit linked to PR - still create CommitSource, PR info is available via get_pull_requests()
            # Check if PR is merged
            if not pr.merged:
                if not self.allow_unmerged:
                    raise ValueError(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged. Cannot backport unmerged PR. Use --allow-unmerged to allow")
                else:
                    self.logger.info(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged, but --allow-unmerged is set, proceeding")
        
        # Commit not linked to PR or linked but we use commit
        if not pulls.totalCount > 0:
            # Commit not linked to PR
            if is_merge_commit:
                # Merge commit without linked PR
                if self.merge_commits_mode == 'fail':
                    raise ValueError(f"Commit {expanded_sha[:7]} is a merge commit without associated PR. Use PR number instead or set --merge-commits skip")
                elif self.merge_commits_mode == 'skip':
                    self.logger.info(f"Skipping merge commit {expanded_sha[:7]} (--merge-commits skip, no associated PR)")
                    return
        
        # Create CommitSource and add to sources
        source = CommitSource(commit, self.repo, self.merge_commits_mode)
        self.source_info.sources.append(source)
    
    def _add_pull(self, p: int, single: bool):
        """Adds PR and gets commits from it"""
        pull = self.repo.get_pull(p)
        
        if not pull.merged:
            if not self.allow_unmerged:
                raise ValueError(f"PR #{p} is not merged. Use --allow-unmerged to backport unmerged PRs")
            else:
                self.logger.info(f"PR #{p} is not merged, but --allow-unmerged is set, proceeding with commits from PR")
        
        # Create PRSource and add to sources
        source = PRSource(pull, self.repo, self.merge_commits_mode, self.allow_unmerged)
        self.source_info.sources.append(source)
        
        # Validate that PR has commits (this will also populate _commit_shas)
        try:
            commit_shas = source.get_commit_shas()
            if not pull.merged:
                self.logger.info(f"PR #{p} is unmerged, using {len(commit_shas)} commits from PR")
            elif pull.merge_commit_sha:
                merge_commit = self.repo.get_commit(pull.merge_commit_sha)
                if merge_commit.parents and len(merge_commit.parents) > 1:
                    self.logger.info(f"PR #{p} was merged as merge commit, using {len(commit_shas)} individual commits")
                else:
                    self.logger.info(f"PR #{p} was merged as squash/rebase, using merge_commit_sha")
        except ValueError as e:
            raise
    
    def process(self):
        """Main processing method"""
        # Validation
        try:
            self.validator.validate_all(
                self.source_info.commit_shas,
                self.target_branches,
                self.source_info.pull_requests
            )
        except ValueError as e:
            error_msg = f"VALIDATION_ERROR: {e}"
            self.logger.error(error_msg)
            self.summary_writer.write(error_msg)
            sys.exit(1)
        
        # Create initial comment
        self.comment_manager.create_initial_comment(
            self.source_info.pull_requests,
            self.target_branches,
            self.workflow_url
        )
        
        # Clone repository
        try:
            repo_url = f"https://{self.token}@github.com/{self.repo_name}.git"
            self.git_repo.clone(repo_url, "ydb-new-pr")
            # Update repo path after cloning
            self.git_repo.repo = git.Repo("ydb-new-pr")
        except (git.GitCommandError, Exception) as e:
            error_msg = f"REPOSITORY_CLONE_ERROR: Failed to clone repository {self.repo_name}: {e}"
            self.logger.error(error_msg)
            self.summary_writer.write(error_msg)
            sys.exit(1)
        
        os.chdir("ydb-new-pr")
        
        # Process each target branch
        for target_branch in self.target_branches:
            try:
                result = self._process_branch(target_branch)
                self.results.append(result)
            except Exception as e:
                self.has_errors = True
                error_msg = f"UNEXPECTED_ERROR: Branch {target_branch} - {type(e).__name__}: {e}"
                self.logger.error(error_msg)
                self.summary_writer.write(f"Branch {target_branch} error: {type(e).__name__}\n```\n{e}\n```")
                self.skipped_branches.append((target_branch, f"unexpected error: {type(e).__name__}"))
        
        # Update comments
        self.comment_manager.update_with_results(
            self.results,
            self.skipped_branches,
            self.target_branches,
            self.workflow_url
        )
        
        # Check errors
        if self.has_errors:
            error_msg = "WORKFLOW_FAILED: Cherry-pick workflow completed with errors. Check logs above for details."
            self.logger.error(error_msg)
            self.summary_writer.write(error_msg)
            sys.exit(1)
        
        self.logger.info("WORKFLOW_SUCCESS: All cherry-pick operations completed successfully")
        self.summary_writer.write("All cherry-pick operations completed successfully")
    
    def _process_branch(self, target_branch: str) -> BackportResult:
        """Processes single branch"""
        dev_branch_name = f"cherry-pick-{target_branch}-{self.dtm}"
        all_conflict_files: List[ConflictInfo] = []
        cherry_pick_logs: List[str] = []
        
        # Prepare branch
        self.git_repo.fetch("origin", target_branch)
        self.git_repo.reset_hard()
        self.git_repo.checkout_branch(target_branch, f"origin/{target_branch}")
        self.git_repo.create_branch(dev_branch_name, target_branch)
        
        # Cherry-pick each commit
        for commit_sha in self.source_info.commit_shas:
            result = self.git_repo.cherry_pick(commit_sha)
            
            # Save raw log
            if result.raw_output:
                cherry_pick_logs.append(f"=== Cherry-picking {commit_sha[:7]} ===\n{result.raw_output}")
            
            # Handle conflicts
            if result.has_conflicts:
                conflicts = self.conflict_handler.detect_conflicts(result.raw_output)
                if self.conflict_handler.commit_conflicts(commit_sha, conflicts):
                    all_conflict_files.extend(conflicts)
                else:
                    # Failed to commit conflicts
                    self.git_repo.cherry_pick_abort()
                    self.has_errors = True
                    error_msg = f"CHERRY_PICK_CONFLICT_ERROR: Failed to handle conflict for commit {commit_sha[:7]} in branch {target_branch}"
                    self.logger.error(error_msg)
                    self.summary_writer.write(error_msg)
                    self.skipped_branches.append((target_branch, "cherry-pick conflict (failed to resolve)"))
                    raise RuntimeError(error_msg)
        
        # Push branch
        try:
            self.git_repo.push(dev_branch_name, set_upstream=True)
        except subprocess.CalledProcessError:
            self.has_errors = True
            raise
        
        # Create PR
        has_conflicts = len(all_conflict_files) > 0
        
        # Create context for PR
        context = PRContext(
            target_branch=target_branch,
            dev_branch_name=dev_branch_name,
            repo_name=self.repo_name,
            source_info=self.source_info,
            has_conflicts=has_conflicts,
            conflict_files=all_conflict_files,
            cherry_pick_logs=cherry_pick_logs,
            workflow_url=self.workflow_url,
            workflow_triggerer=self.workflow_triggerer
        )
        
        title = self.pr_builder.build_title(target_branch, self.source_info, has_conflicts)
        body = self.pr_builder.build_body(context)
        
        try:
            pr = self.github_client.create_pr(
                base=target_branch,
                head=dev_branch_name,
                title=title,
                body=body,
                draft=has_conflicts
            )
            context.pr_number = pr.number
            
            # Update body with PR number for correct links
            if has_conflicts:
                updated_body = self.pr_builder.build_body(context)
                pr.edit(body=updated_body)
            
            # Assign assignee
            if self.workflow_triggerer and self.workflow_triggerer != 'unknown':
                try:
                    pr.add_to_assignees(self.workflow_triggerer)
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
            self.summary_writer.write_branch_result(
                target_branch,
                pr,
                has_conflicts,
                all_conflict_files,
                cherry_pick_logs
            )
            
            return BackportResult(
                target_branch=target_branch,
                pr=pr,
                conflict_files=all_conflict_files,
                cherry_pick_logs=cherry_pick_logs
            )
        except GithubException as e:
            self.has_errors = True
            self.logger.error(f"PR_CREATION_ERROR: Failed to create PR for branch {target_branch}: {e}")
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--commits",
        help="List of commits to cherry-pick. Can be represented as full or short commit SHA, PR number or URL to commit or PR. Separated by space, comma or line end.",
    )
    parser.add_argument(
        "--target-branches", help="List of branches to cherry-pick. Separated by space, comma or line end."
    )
    parser.add_argument(
        "--merge-commits",
        choices=['fail', 'skip'],
        default='skip',
        help="How to handle merge commits inside PR: 'skip' (default) - skip merge commits, 'fail' - fail on merge commits"
    )
    parser.add_argument(
        "--allow-unmerged",
        action='store_true',
        help="Allow backporting unmerged PRs (uses commits from PR directly, not merge_commit_sha)"
    )
    args = parser.parse_args()

    log_fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(format=log_fmt, level=logging.DEBUG)
    
    # Use new orchestrator
    repo_name = os.environ["REPO"]
    token = os.environ["TOKEN"]
    workflow_triggerer = os.environ.get('GITHUB_ACTOR', 'unknown')
    
    # Get workflow URL
    workflow_url = None
    run_id = os.getenv('GITHUB_RUN_ID')
    if run_id:
        try:
            gh = Github(login_or_token=token)
            repo = gh.get_repo(repo_name)
            workflow_url = repo.get_workflow_run(int(run_id)).html_url
        except (GithubException, ValueError):
            pass
    
    orchestrator = CherryPickOrchestrator(args, repo_name, token, workflow_triggerer, workflow_url)
    orchestrator.process()


if __name__ == "__main__":
    main()