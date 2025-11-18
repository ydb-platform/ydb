<<<<<<< HEAD
=======
#!/usr/bin/env python3
import os
import sys
import datetime
import logging
import subprocess
import argparse
import re
from typing import List, Optional, Tuple
from github import Github, GithubException, GithubObject, Commit
import requests


class CherryPickCreator:
    def __init__(self, args):
        def __split(s: str, seps: str = ', \n'):
            if not s:
                return []
            if not seps:
                return [s]
            result = []
            for part in s.split(seps[0]):
                result += __split(part, seps[1:])
            return result

        self.repo_name = os.environ["REPO"]
        self.target_branches = __split(args.target_branches)
        self.token = os.environ["TOKEN"]
        self.gh = Github(login_or_token=self.token)
        self.repo = self.gh.get_repo(self.repo_name)
        self.commit_shas: list[str] = []
        self.pr_title_list: list[str] = []
        self.pr_body_list: list[str] = []
        self.pull_requests: list = []  # Store PR objects for later use
        self.pull_authors: list[str] = []
        self.workflow_triggerer = os.environ.get('GITHUB_ACTOR', 'unknown')
        self.has_errors = False
        self.merge_commits_mode = getattr(args, 'merge_commits', 'skip')  # fail or skip
        self.allow_unmerged = getattr(args, 'allow_unmerged', False)  # Allow backporting unmerged PRs
        self.created_backport_prs = []  # Store created backport PRs: [(target_branch, pr, has_conflicts), ...]
        self.skipped_branches = []  # Store branches where PR was not created: [(target_branch, reason), ...]
        self.backport_comments = []  # Store comment objects for editing: [(pull, comment), ...]
        
        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M")
        self.logger = logging.getLogger("cherry-pick")
        # Get workflow run URL
        run_id = os.getenv('GITHUB_RUN_ID')
        if run_id:
            try:
                self.workflow_url = self.repo.get_workflow_run(int(run_id)).html_url
            except (GithubException, ValueError):
                self.workflow_url = None
        else:
            self.workflow_url = None

        commits = __split(args.commits)
        for c in commits:
            id = c.split('/')[-1].strip()
            try:
                self.__add_pull(int(id), len(commits) == 1)
            except ValueError:
                self.__add_commit(id, len(commits) == 1)

    def _expand_short_sha(self, short_sha: str) -> str:
        """Expand short SHA to full SHA via GitHub API or git rev-parse"""
        if len(short_sha) == 40:
            return short_sha  # already full
        
        if len(short_sha) < 7:
            raise ValueError(f"SHA too short: {short_sha}. Minimum 7 characters required")
        
        # Try GitHub API first (works before cloning)
        try:
            commits = self.repo.get_commits(sha=short_sha)
            if commits.totalCount > 0:
                commit = commits[0]
                if commit.sha.startswith(short_sha):
                    return commit.sha
        except Exception as e:
            self.logger.debug(f"Failed to find commit via GitHub API: {e}")
        
        # If GitHub API didn't find it, use git rev-parse (after cloning)
        result = subprocess.run(
            ['git', 'rev-parse', short_sha],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()

    def __add_commit(self, c: str, single: bool):
        """Add commit by SHA (supports short SHA)"""
        # Expand short SHA to full
        try:
            expanded_sha = self._expand_short_sha(c)
        except ValueError as e:
            self.logger.error(f"Failed to expand SHA {c}: {e}")
            raise
        
        commit = self.repo.get_commit(expanded_sha)
        
        # Check if this is a merge commit
        is_merge_commit = commit.parents and len(commit.parents) > 1
        
        if is_merge_commit:
            if self.merge_commits_mode == 'fail':
                raise ValueError(f"Commit {expanded_sha[:7]} is a merge commit. Use PR number instead or set --merge-commits skip")
            elif self.merge_commits_mode == 'skip':
                self.logger.info(f"Skipping merge commit {expanded_sha[:7]} (--merge-commits skip)")
                return
        
        pulls = commit.get_pulls()
        if pulls.totalCount > 0:
            pr = pulls.get_page(0)[0]
            # Check that PR was merged (unless allow_unmerged is set)
            if not pr.merged:
                if not self.allow_unmerged:
                    raise ValueError(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged. Cannot backport unmerged PR. Use --allow-unmerged to allow")
                else:
                    self.logger.info(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged, but --allow-unmerged is set, proceeding")
            # Add PR to pull_requests if not already added (for comments and issues)
            existing_pr_numbers = [p.number for p in self.pull_requests]
            if pr.number not in existing_pr_numbers:
                self.pull_requests.append(pr)
                self.pull_authors.append(pr.user.login)
            if single:
                self.pr_title_list.append(pr.title)
            else:
                self.pr_title_list.append(f'commit {commit.sha[:7]}')
            self.pr_body_list.append(f"* commit {commit.html_url}: {pr.title}")
        else:
            # Try to get commit author if no PR found
            try:
                commit_author = commit.author.login if commit.author else None
                if commit_author and commit_author not in self.pull_authors:
                    self.pull_authors.append(commit_author)
            except:
                pass
            if single:
                self.pr_title_list.append(f'cherry-pick commit {commit.sha[:7]}')
            else:
                self.pr_title_list.append(f'commit {commit.sha[:7]}')
            self.pr_body_list.append(f"* commit {commit.html_url}")
        self.commit_shas.append(commit.sha)

    def _get_commits_from_pr(self, pull) -> List[str]:
        """Get list of commits from PR (excluding merge commits)"""
        commits = []
        for commit in pull.get_commits():
            commit_obj = commit.commit
            # Skip merge commits inside PR if mode is skip
            if self.merge_commits_mode == 'skip':
                # Check if commit is a merge commit
                if commit_obj.parents and len(commit_obj.parents) > 1:
                    self.logger.info(f"Skipping merge commit {commit.sha[:7]} from PR #{pull.number}")
                    continue
            commits.append(commit.sha)
        return commits

    def __add_pull(self, p: int, single: bool):
        """Add PR and get commits from it"""
        pull = self.repo.get_pull(p)
        
        # Check that PR was merged (unless allow_unmerged is set)
        if not pull.merged:
            if not self.allow_unmerged:
                raise ValueError(f"PR #{p} is not merged. Use --allow-unmerged to backport unmerged PRs")
            else:
                self.logger.info(f"PR #{p} is not merged, but --allow-unmerged is set, proceeding with commits from PR")
        
        self.pull_requests.append(pull)
        self.pull_authors.append(pull.user.login)
        
        if single:
            self.pr_title_list.append(f"{pull.title}")
        else:
            self.pr_title_list.append(f'PR {pull.number}')
        self.pr_body_list.append(f"* PR {pull.html_url}")
        
        # Get commits from PR instead of merge_commit_sha
        pr_commits = self._get_commits_from_pr(pull)
        if not pr_commits:
            raise ValueError(f"PR #{p} contains no commits to cherry-pick")
        
        # For unmerged PRs, always use commits from PR (no merge_commit_sha)
        if not pull.merged:
            self.commit_shas.extend(pr_commits)
            self.logger.info(f"PR #{p} is unmerged, using {len(pr_commits)} commits from PR")
        elif pull.merge_commit_sha:
            # If PR was merged as merge commit, use individual commits
            # Otherwise use merge_commit_sha (for squash/rebase merge)
            # Check if merge_commit_sha is a merge commit
            merge_commit = self.repo.get_commit(pull.merge_commit_sha)
            if merge_commit.parents and len(merge_commit.parents) > 1:
                # This is a merge commit, use individual commits from PR
                self.commit_shas.extend(pr_commits)
                self.logger.info(f"PR #{p} was merged as merge commit, using {len(pr_commits)} individual commits")
            else:
                # This is not a merge commit (squash/rebase), use merge_commit_sha
                self.commit_shas.append(pull.merge_commit_sha)
                self.logger.info(f"PR #{p} was merged as squash/rebase, using merge_commit_sha")
        else:
            # If merge_commit_sha is missing, use commits from PR
            self.commit_shas.extend(pr_commits)

    def _validate_prs(self):
        """Validate PR numbers"""
        # Validate PRs from self.pull_requests (already loaded)
        for pull in self.pull_requests:
            try:
                pull = self.repo.get_pull(pull.number)
                if not pull.merged and not self.allow_unmerged:
                    raise ValueError(f"PR #{pull.number} is not merged. Use --allow-unmerged to allow backporting unmerged PRs")
            except GithubException as e:
                raise ValueError(f"PR #{pull.number} does not exist or is not accessible: {e}")

    def _validate_commits(self):
        """Validate commits (SHA)"""
        for commit_sha in self.commit_shas:
            try:
                commit = self.repo.get_commit(commit_sha)
                # Check that commit exists
                if not commit.sha:
                    raise ValueError(f"Commit {commit_sha} not found")
            except GithubException as e:
                raise ValueError(f"Commit {commit_sha} does not exist: {e}")

    def _validate_branches(self):
        """Validate target branches"""
        for branch in self.target_branches:
            try:
                self.repo.get_branch(branch)
            except GithubException as e:
                raise ValueError(f"Branch {branch} does not exist: {e}")

    def _validate_inputs(self):
        """Validate all input data"""
        if len(self.commit_shas) == 0:
            raise ValueError("No commits to cherry-pick")
        if len(self.target_branches) == 0:
            raise ValueError("No target branches specified")
        
        self.logger.info("Validating input data...")
        self._validate_prs()
        self._validate_commits()
        self._validate_branches()
        self.logger.info("Input validation successful")

    def _is_commit_in_branch(self, commit_sha: str, branch_name: str) -> bool:
        """Check if commit already exists in target branch"""
        try:
            # First fetch for up-to-date branch information
            self.git_run("fetch", "origin", branch_name)
            
            # Check via merge-base
            result = subprocess.run(
                ['git', 'merge-base', '--is-ancestor', commit_sha, f'origin/{branch_name}'],
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except Exception as e:
            self.logger.warning(f"Failed to check if commit {commit_sha[:7]} exists in branch {branch_name}: {e}")
            return False

    def _get_linked_issues_graphql(self, pr_number: int) -> List[str]:
        """Get linked issues via GraphQL API"""
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
        
        owner, repo_name = self.repo_name.split('/')
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

    def _get_linked_issues(self) -> str:
        """Get linked issues for all PRs"""
        all_issues = []
        
        for pull in self.pull_requests:
            issues = self._get_linked_issues_graphql(pull.number)
            
            # If GraphQL didn't return issues, parse from PR body
            if not issues and pull.body:
                body_issues = re.findall(r'#(\d+)', pull.body)
                issues = [f"#{num}" for num in body_issues]
            
            all_issues.extend(issues)
        
        # Remove duplicates
        unique_issues = list(dict.fromkeys(all_issues))
        return ' '.join(unique_issues) if unique_issues else 'None'

    def _extract_changelog_category(self, pr_body: str) -> Optional[str]:
        """Extract Changelog category from PR body"""
        if not pr_body:
            return None
        
        # Match the category section
        category_match = re.search(r"### Changelog category.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
        if not category_match:
            return None
        
        # Extract categories (lines starting with *)
        categories = [line.strip('* ').strip() for line in category_match.group(1).splitlines() if line.strip() and line.strip().startswith('*')]
        
        # Find the selected category - take the first non-empty category line
        # The selected category is typically the one that's not commented out
        for cat in categories:
            cat_clean = cat.strip('* ').strip()
            if cat_clean:
                # Return the category as-is, without hardcoded validation
                return cat_clean
        
        return None

    def _extract_changelog_entry(self, pr_body: str) -> Optional[str]:
        """Extract Changelog entry from PR body"""
        if not pr_body:
            return None
        
        entry_match = re.search(r"### Changelog entry.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
        if not entry_match:
            return None
        
        entry = entry_match.group(1).strip()
        # Skip if it's just "..." or empty
        if entry in ['...', '']:
            return None
        
        return entry

    def _extract_description_for_reviewers(self, pr_body: str) -> Optional[str]:
        """Extract Description for reviewers section from PR body"""
        if not pr_body:
            return None
        
        desc_match = re.search(r"### Description for reviewers.*?\n(.*?)(\n###|$)", pr_body, re.DOTALL)
        if not desc_match:
            return None
        
        desc = desc_match.group(1).strip()
        # Skip if it's just "..." or empty
        if desc in ['...', '']:
            return None
        
        return desc

    def _get_changelog_info(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Get Changelog category, entry, and description from original PRs"""
        category = None
        entry = None
        description = None
        
        # Try to get from first PR (usually there's one main PR)
        for pull in self.pull_requests:
            if pull.body:
                cat = self._extract_changelog_category(pull.body)
                if cat:
                    category = cat
                ent = self._extract_changelog_entry(pull.body)
                if ent:
                    entry = ent
                desc = self._extract_description_for_reviewers(pull.body)
                if desc:
                    description = desc
                # If we found all, we're done
                if category and entry and description:
                    break
        
        return category, entry, description

    def pr_title(self, target_branch) -> str:
        """Generate PR title"""
        if len(self.pr_title_list) == 1:
            title = f"[Backport {target_branch}] {self.pr_title_list[0]}"
        else:
            title = f"[Backport {target_branch}] cherry-pick {', '.join(self.pr_title_list)}"
        
        # Truncate long titles (GitHub limit ~256 characters)
        if len(title) > 200:
            title = title[:197] + "..."
        
        return title

    def pr_body(self, with_wf: bool, has_conflicts: bool = False, target_branch: Optional[str] = None) -> str:
        """Generate PR body with improved format that passes validation"""
        commits = '\n'.join(self.pr_body_list)
        
        # Get linked issues
        issue_refs = self._get_linked_issues()
        
        # Format author information
        authors = ', '.join([f"@{author}" for author in set(self.pull_authors)]) if self.pull_authors else "Unknown"
        
        # Determine target branch for description
        branch_desc = target_branch if target_branch else (', '.join(self.target_branches) if len(self.target_branches) == 1 else 'multiple branches')
        
        # Get Changelog category, entry, and description from original PR
        changelog_category, changelog_entry, original_description = self._get_changelog_info()
        
        # Build changelog entry if not found
        if not changelog_entry:
            if len(self.pull_requests) == 1:
                pr_num = self.pull_requests[0].number
                changelog_entry = f"Backport of PR #{pr_num} to `{branch_desc}`"
            else:
                pr_nums = ', '.join([f"#{p.number}" for p in self.pull_requests])
                changelog_entry = f"Backport of PRs {pr_nums} to `{branch_desc}`"
        
        # Ensure entry is at least 20 characters (validation requirement)
        if len(changelog_entry) < 20:
            changelog_entry = f"Backport to `{branch_desc}`: {changelog_entry}"
        
        # For Bugfix category, ensure there's an issue reference in changelog entry (validation requirement)
        if changelog_category and changelog_category.startswith("Bugfix") and issue_refs and issue_refs != "None":
            # Check if issue reference is already in entry
            issue_patterns = [
                r"https://github.com/ydb-platform/[a-z\-]+/issues/\d+",
                r"https://st.yandex-team.ru/[a-zA-Z]+-\d+",
                r"#\d+",
                r"[a-zA-Z]+-\d+"
            ]
            has_issue = any(re.search(pattern, changelog_entry) for pattern in issue_patterns)
            if not has_issue:
                # Add issue reference to entry
                changelog_entry = f"{changelog_entry} ({issue_refs})"
        
        # Build category section - use template if category not found
        if changelog_category:
            category_section = f"* {changelog_category}"
        else:
            # Use full template if category not found - let user choose
            category_section = """* New feature
* Experimental feature
* Improvement
* Performance improvement
* User Interface
* Bugfix
* Backward incompatible change
* Documentation (changelog entry is not required)
* Not for changelog (changelog entry is not required)"""
        
        # Build description for reviewers section
        description_section = f"Backport to `{branch_desc}`.\n\n"
        description_section += f"#### Original PR(s)\n{commits}\n\n"
        description_section += f"#### Metadata\n"
        description_section += f"- **Original PR author(s):** {authors}\n"
        description_section += f"- **Cherry-picked by:** @{self.workflow_triggerer}\n"
        description_section += f"- **Related issues:** {issue_refs}"
        
        # If original PR had description, append it
        if original_description:
            description_section += f"\n\n#### Original Description\n{original_description}"
        
        # Add conflicts section if needed
        if has_conflicts:
            description_section += """
#### Conflicts Require Manual Resolution

This PR contains merge conflicts that require manual resolution.

**How to resolve conflicts:**

```bash
git fetch origin <branch_name>
git checkout <branch_name>
# Resolve conflicts in files
git add .
git commit --amend  # or create new commit
git push
```

After resolving conflicts, mark this PR as ready for review.
"""
        
        # Add workflow link if needed
        if with_wf:
            if self.workflow_url:
                description_section += f"\n\n---\n\nPR was created by cherry-pick workflow [run]({self.workflow_url})"
            else:
                description_section += "\n\n---\n\nPR was created by cherry-pick script"
        
        # Build PR body according to template (Changelog entry and category must come first)
        pr_body = f"""### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

{changelog_entry}

### Changelog category <!-- remove all except one -->

{category_section}

### Description for reviewers <!-- (optional) description for those who read this PR -->

{description_section}
"""
        
        return pr_body
    
    def add_summary(self, msg):
        self.logger.info(msg)
        summary_path = os.getenv('GITHUB_STEP_SUMMARY')
        if summary_path:
            with open(summary_path, 'a') as summary:
                summary.write(f'{msg}\n\n')

    def git_run(self, *args):
        args = ["git"] + list(args)

        self.logger.info("Executing git command: %r", args)
        try:
            output = subprocess.check_output(args).decode()
        except subprocess.CalledProcessError as e:
            error_output = e.output.decode() if e.output else ""
            stderr_output = e.stderr.decode() if e.stderr else ""
            self.logger.error(f"Git command failed: {' '.join(args)}")
            self.logger.error(f"Exit code: {e.returncode}")
            if error_output:
                self.logger.error(f"Stdout: {error_output}")
            if stderr_output:
                self.logger.error(f"Stderr: {stderr_output}")
            raise
        else:
            self.logger.debug("Command output:\n%s", output)
        return output

    def _handle_cherry_pick_conflict(self, commit_sha: str):
        """Handle cherry-pick conflict: commit the conflict"""
        try:
            # Check if there are conflicts (files with conflicts or unmerged paths)
            result = subprocess.run(
                ['git', 'status', '--porcelain'],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Also check for conflict markers
            has_conflict_markers = False
            if result.stdout.strip():
                # Check for conflict markers in files
                status_lines = result.stdout.strip().split('\n')
                for line in status_lines:
                    if line.startswith('UU') or line.startswith('AA') or line.startswith('DD'):
                        # Unmerged paths - definitely a conflict
                        has_conflict_markers = True
                        break
                
                if has_conflict_markers or result.stdout.strip():
                    # There are changes (conflicts)
                    self.git_run("add", "-A")
                    self.git_run("commit", "-m", f"BACKPORT-CONFLICT: manual resolution required for commit {commit_sha[:7]}")
                    self.logger.info(f"Conflict committed for commit {commit_sha[:7]}")
                    return True
        except Exception as e:
            self.logger.error(f"Error handling conflict for commit {commit_sha[:7]}: {e}")
        
        return False

    def _create_initial_backport_comment(self):
        """Create initial comment in original PRs about backport start"""
        if not self.workflow_url:
            self.logger.warning("Workflow URL not available, skipping initial comment")
            return
        
        target_branches_str = ', '.join([f"`{b}`" for b in self.target_branches])
        initial_comment = f"Backport to {target_branches_str} in progress: [workflow run]({self.workflow_url})"
        
        for pull in self.pull_requests:
            try:
                comment = pull.create_issue_comment(initial_comment)
                self.backport_comments.append((pull, comment))
                self.logger.info(f"Created initial backport comment in original PR #{pull.number}")
            except GithubException as e:
                self.logger.warning(f"Failed to create initial comment in original PR #{pull.number}: {e}")

    def _update_backport_comment(self):
        """Update comment in original PRs with backport results"""
        if not self.backport_comments:
            return
        
        for pull, comment in self.backport_comments:
            try:
                total_branches = len(self.created_backport_prs) + len(self.skipped_branches)
                
                if total_branches == 0:
                    # No branches processed (should not happen)
                    updated_comment = f"Backport to {', '.join([f'`{b}`' for b in self.target_branches])} completed with no results"
                    if self.workflow_url:
                        updated_comment += f" - [workflow run]({self.workflow_url})"
                elif total_branches == 1 and len(self.created_backport_prs) == 1:
                    # Single branch with PR - simple comment
                    target_branch, pr, has_conflicts = self.created_backport_prs[0]
                    status = "draft PR" if has_conflicts else "PR"
                    updated_comment = f"Backported to `{target_branch}`: {status} {pr.html_url}"
                    if has_conflicts:
                        updated_comment += " (contains conflicts requiring manual resolution)"
                    if self.workflow_url:
                        updated_comment += f" - [workflow run]({self.workflow_url})"
                else:
                    # Multiple branches or mixed results - list all
                    updated_comment = "Backport results:\n"
                    
                    # List created PRs
                    for target_branch, pr, has_conflicts in self.created_backport_prs:
                        status = "draft PR" if has_conflicts else "PR"
                        conflict_note = " (contains conflicts requiring manual resolution)" if has_conflicts else ""
                        updated_comment += f"- `{target_branch}`: {status} {pr.html_url}{conflict_note}\n"
                    
                    # List skipped branches
                    for target_branch, reason in self.skipped_branches:
                        updated_comment += f"- `{target_branch}`: skipped ({reason})\n"
                    
                    if self.workflow_url:
                        updated_comment += f"\n[workflow run]({self.workflow_url})"
                
                comment.edit(updated_comment)
                self.logger.info(f"Updated backport comment in original PR #{pull.number}")
            except GithubException as e:
                self.logger.warning(f"Failed to update comment in original PR #{pull.number}: {e}")

    def create_pr_for_branch(self, target_branch):
        """Create PR for target branch with conflict handling"""
        dev_branch_name = f"cherry-pick-{target_branch}-{self.dtm}"
        has_conflicts = False
        
        # First fetch for up-to-date branch information
        self.git_run("fetch", "origin", target_branch)
        
        self.git_run("reset", "--hard")
        # Create/update local branch based on origin/target_branch (-B overwrites if exists)
        self.git_run("checkout", "-B", target_branch, f"origin/{target_branch}")
        self.git_run("checkout", "-b", dev_branch_name)
        
        # Filter commits that already exist in branch
        commits_to_pick = []
        for commit_sha in self.commit_shas:
            if self._is_commit_in_branch(commit_sha, target_branch):
                self.logger.info(f"Commit {commit_sha[:7]} already exists in branch {target_branch}, skipping")
                self.add_summary(f"Commit {commit_sha[:7]} already exists in branch {target_branch}, skipped")
            else:
                commits_to_pick.append(commit_sha)
        
        if not commits_to_pick:
            reason = "all commits already exist in target branch"
            self.logger.info(f"All commits already exist in branch {target_branch}, skipping PR creation")
            self.add_summary(f"All commits already exist in branch {target_branch}, skipping PR creation")
            self.skipped_branches.append((target_branch, reason))
            return
        
        # Cherry-pick commits
        for commit_sha in commits_to_pick:
            try:
                self.git_run("cherry-pick", "--allow-empty", commit_sha)
            except subprocess.CalledProcessError as e:
                error_output = e.output.decode() if e.output else ""
                # Check if this is a conflict or another error
                if "CONFLICT" in error_output or "conflict" in error_output.lower():
                    self.logger.warning(f"Conflict during cherry-pick of commit {commit_sha[:7]}")
                    if self._handle_cherry_pick_conflict(commit_sha):
                        has_conflicts = True
                    else:
                        # Failed to handle conflict, abort cherry-pick
                        self.git_run("cherry-pick", "--abort")
                        self.has_errors = True
                        error_msg = f"CHERRY_PICK_CONFLICT_ERROR: Failed to handle conflict for commit {commit_sha[:7]} in branch {target_branch}"
                        self.logger.error(error_msg)
                        self.add_summary(error_msg)
                else:
                    # Another error
                    self.git_run("cherry-pick", "--abort")
                    raise
        
        # Push branch
        try:
            self.git_run("push", "--set-upstream", "origin", dev_branch_name)
        except subprocess.CalledProcessError:
            self.has_errors = True
            raise

        # Create PR (draft if there are conflicts)
        try:
            pr = self.repo.create_pull(
                base=target_branch,
                head=dev_branch_name,
                title=self.pr_title(target_branch),
                body=self.pr_body(True, has_conflicts, target_branch),
                maintainer_can_modify=True,
                draft=has_conflicts
            )
        except GithubException as e:
            self.has_errors = True
            self.logger.error(f"PR_CREATION_ERROR: Failed to create PR for branch {target_branch}: {e}")
            raise

        # Assign workflow triggerer as assignee
        if self.workflow_triggerer and self.workflow_triggerer != 'unknown':
            try:
                pr.add_to_assignees(self.workflow_triggerer)
                self.logger.info(f"Assigned {self.workflow_triggerer} as assignee to PR {pr.html_url}")
            except GithubException as e:
                self.logger.warning(f"Failed to assign {self.workflow_triggerer} as assignee to PR {pr.html_url}: {e}")

        if has_conflicts:
            self.logger.info(f"Created draft PR {pr.html_url} for branch {target_branch} with conflicts")
            self.add_summary(f"Branch {target_branch}: Draft PR {pr.html_url} created with conflicts")
        else:
            self.logger.info(f"Created PR {pr.html_url} for branch {target_branch}")
            self.add_summary(f"Branch {target_branch}: PR {pr.html_url} created")

        # Enable automerge only if there are no conflicts
        if not has_conflicts:
            try:
                pr.enable_automerge(merge_method='MERGE')
            except BaseException as e:
                self.logger.warning(f"AUTOMERGE_WARNING: Failed to enable automerge with method MERGE for PR {pr.html_url}: {e}")
                try:
                    pr.enable_automerge(merge_method='SQUASH')
                except BaseException as f:
                    self.logger.warning(f"AUTOMERGE_WARNING: Failed to enable automerge with method SQUASH for PR {pr.html_url}: {f}")
        
        # Store created PR for later comment
        self.created_backport_prs.append((target_branch, pr, has_conflicts))

    def process(self):
        """Main processing method"""
        br = ', '.join([f'[{b}]({self.repo.html_url}/tree/{b})' for b in self.target_branches])
        self.logger.info(f"Starting cherry-pick process: {self.pr_title(br)}")
        self.add_summary(f"{self.pr_body(False)}to {br}")
        
        # Create initial comment in original PRs
        self._create_initial_backport_comment()
        
        # Validate input data
        try:
            self._validate_inputs()
        except ValueError as e:
            error_msg = f"VALIDATION_ERROR: {e}"
            self.logger.error(error_msg)
            self.add_summary(error_msg)
            sys.exit(1)
        except Exception as e:
            error_msg = f"VALIDATION_ERROR: Critical validation error: {e}"
            self.logger.error(error_msg)
            self.add_summary(error_msg)
            sys.exit(1)
        
        # Clone repository
        try:
            self.git_run(
                "clone", f"https://{self.token}@github.com/{self.repo_name}.git", "-c", "protocol.version=2", f"ydb-new-pr"
            )
        except subprocess.CalledProcessError as e:
            error_msg = f"REPOSITORY_CLONE_ERROR: Failed to clone repository {self.repo_name}: {e}"
            self.logger.error(error_msg)
            self.add_summary(error_msg)
            sys.exit(1)
        
        os.chdir(f"ydb-new-pr")
        
        # Process each target branch
        for target in self.target_branches:
            try:
                self.create_pr_for_branch(target)
            except GithubException as e:
                self.has_errors = True
                error_msg = f"GITHUB_API_ERROR: Branch {target} - {type(e).__name__}: {e}"
                self.logger.error(error_msg)
                self.add_summary(f"Branch {target} error: {type(e).__name__}\n```\n{e}\n```")
                # Track failed branch
                reason = f"GitHub API error: {str(e)[:100]}"  # Truncate long error messages
                self.skipped_branches.append((target, reason))
            except subprocess.CalledProcessError as e:
                self.has_errors = True
                error_msg = f"GIT_COMMAND_ERROR: Branch {target} - Command failed with exit code {e.returncode}"
                self.logger.error(error_msg)
                if e.output:
                    self.logger.error(f"Command stdout: {e.output.decode()}")
                if e.stderr:
                    self.logger.error(f"Command stderr: {e.stderr.decode()}")
                summary_msg = f"Branch {target} error: subprocess.CalledProcessError (exit code {e.returncode})"
                if e.output:
                    summary_msg += f'\nStdout:\n```\n{e.output.decode()}\n```'
                if e.stderr:
                    summary_msg += f'\nStderr:\n```\n{e.stderr.decode()}\n```'
                self.add_summary(summary_msg)
                # Track failed branch
                error_output = e.output.decode() if e.output else ""
                if "CONFLICT" in error_output or "conflict" in error_output.lower():
                    reason = "cherry-pick conflict (failed to resolve)"
                else:
                    reason = f"git command failed (exit code {e.returncode})"
                self.skipped_branches.append((target, reason))
            except BaseException as e:
                self.has_errors = True
                error_msg = f"UNEXPECTED_ERROR: Branch {target} - {type(e).__name__}: {e}"
                self.logger.error(error_msg)
                self.add_summary(f"Branch {target} error: {type(e).__name__}\n```\n{e}\n```")
                # Track failed branch
                reason = f"unexpected error: {type(e).__name__}"
                self.skipped_branches.append((target, reason))
        
        # Update comments in original PRs with backport results
        self._update_backport_comment()
        
        # If there were errors, exit with error
        if self.has_errors:
            error_msg = "WORKFLOW_FAILED: Cherry-pick workflow completed with errors. Check logs above for details."
            self.logger.error(error_msg)
            self.add_summary(error_msg)
            sys.exit(1)
        
        self.logger.info("WORKFLOW_SUCCESS: All cherry-pick operations completed successfully")
        self.add_summary("All cherry-pick operations completed successfully")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--commits",
        help="List of commits to cherry-pick. Can be represented as full or short commit SHA, PR number or URL to commit or PR. Separated by space, comma or line end.",
    )
    parser.add_argument(
        "--target-branches", help="List of branchs to cherry-pick. Separated by space, comma or line end."
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
    CherryPickCreator(args).process()


if __name__ == "__main__":
    main()
>>>>>>> 6b621df
