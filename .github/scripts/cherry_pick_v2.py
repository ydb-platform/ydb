#!/usr/bin/env python3
import os
import sys
import datetime
import logging
import subprocess
import argparse
import re
import hashlib
import base64
from typing import List, Optional, Tuple
from urllib.parse import quote
from dataclasses import dataclass, field
from github import Github, GithubException, GithubObject, Commit
import requests

# Import PR template and categories from validate_pr_description action
try:
    pr_template_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'actions', 'validate_pr_description'))
    sys.path.insert(0, pr_template_path)
    from pr_template import (
        ISSUE_PATTERNS,
        FOR_CHANGELOG_CATEGORIES,
        NOT_FOR_CHANGELOG_CATEGORIES,
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
    """Информация о конфликте в файле"""
    file_path: str
    line: Optional[int] = None
    message: Optional[str] = None


@dataclass
class CherryPickResult:
    """Результат выполнения git cherry-pick"""
    success: bool
    has_conflicts: bool
    is_empty: bool = False  # True если "nothing to commit"
    raw_output: str = ""  # Сырой вывод git команды
    conflict_files: List[ConflictInfo] = field(default_factory=list)


@dataclass
class SourceInfo:
    """Информация об исходных PR/commits для backport"""
    titles: List[str] = field(default_factory=list)
    body_items: List[str] = field(default_factory=list)
    pull_requests: List = field(default_factory=list)
    authors: List[str] = field(default_factory=list)
    commit_shas: List[str] = field(default_factory=list)


@dataclass
class PRContext:
    """Контекст для генерации PR body"""
    target_branch: str
    dev_branch_name: str
    repo_name: str
    pr_number: Optional[int] = None
    
    # Source info
    source_prs: List = field(default_factory=list)
    source_info: Optional[SourceInfo] = None  # Для body_items
    authors: List[str] = field(default_factory=list)
    issues: str = ""
    
    # Changelog
    changelog_category: Optional[str] = None
    changelog_entry: str = ""
    
    # Conflicts
    has_conflicts: bool = False
    conflict_files: List[ConflictInfo] = field(default_factory=list)
    
    # Logs
    cherry_pick_logs: List[str] = field(default_factory=list)  # Сырые логи git cherry-pick
    
    # Workflow
    workflow_url: Optional[str] = None
    workflow_triggerer: str = "unknown"


@dataclass
class BackportResult:
    """Результат backport для одной ветки"""
    target_branch: str
    pr = None  # PullRequest object
    has_conflicts: bool = False
    conflict_files: List[ConflictInfo] = field(default_factory=list)
    error: Optional[str] = None
    cherry_pick_logs: List[str] = field(default_factory=list)


# ============================================================================
# Core Classes
# ============================================================================

class CommitResolver:
    """Определяет и резолвит commit references (SHA, short SHA, PR number)"""
    
    MIN_SHA_LENGTH = 7
    FULL_SHA_LENGTH = 40
    
    def __init__(self, repo, logger):
        self.repo = repo
        self.logger = logger
    
    def is_likely_sha(self, ref: str) -> bool:
        """Проверяет, похоже ли на SHA (hexadecimal, правильная длина)"""
        if not ref:
            return False
        # Проверяем что это hex и правильная длина
        try:
            int(ref, 16)  # Проверка hex
            return len(ref) >= self.MIN_SHA_LENGTH and len(ref) <= self.FULL_SHA_LENGTH
        except ValueError:
            return False
    
    def expand_sha(self, ref: str) -> str:
        """Расширяет short SHA до full SHA с валидацией"""
        if len(ref) == self.FULL_SHA_LENGTH and self.is_likely_sha(ref):
            return ref  # Уже полный SHA
        
        if not self.is_likely_sha(ref):
            raise ValueError(f"'{ref}' не похож на SHA (должен быть hex, минимум {self.MIN_SHA_LENGTH} символов)")
        
        # Пробуем GitHub API (работает до клонирования)
        try:
            commits = self.repo.get_commits(sha=ref)
            if commits.totalCount > 0:
                commit = commits[0]
                if commit.sha.startswith(ref):
                    return commit.sha
        except Exception as e:
            self.logger.debug(f"Failed to find commit via GitHub API: {e}")
        
        # Fallback: git rev-parse (требует клонированный репозиторий)
        try:
            result = subprocess.run(
                ['git', 'rev-parse', ref],
                capture_output=True,
                text=True,
                check=True
            )
            expanded = result.stdout.strip()
            if len(expanded) == self.FULL_SHA_LENGTH:
                return expanded
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            pass
        
        raise ValueError(f"Не удалось найти commit для '{ref}'")


class GitRepository:
    """Абстракция над git командами"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def git_run(self, *args):
        """Выполняет git команду с логированием"""
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
    
    def clone(self, repo_url: str, target_dir: str):
        """Клонирует репозиторий"""
        self.git_run("clone", repo_url, "-c", "protocol.version=2", target_dir)
    
    def fetch(self, remote: str, branch: str):
        """Fetch ветки"""
        self.git_run("fetch", remote, branch)
    
    def reset_hard(self):
        """Hard reset текущей ветки"""
        self.git_run("reset", "--hard")
    
    def checkout_branch(self, branch: str, from_branch: Optional[str] = None):
        """Checkout ветки (создает если не существует)"""
        if from_branch:
            self.git_run("checkout", "-B", branch, from_branch)
        else:
            self.git_run("checkout", branch)
    
    def create_branch(self, branch_name: str, from_branch: str):
        """Создает новую ветку"""
        self.git_run("checkout", "-b", branch_name)
        return branch_name
    
    def cherry_pick(self, commit_sha: str) -> CherryPickResult:
        """Cherry-pick commit с сохранением полного вывода"""
        try:
            result = subprocess.run(
                ['git', 'cherry-pick', commit_sha],
                capture_output=True,
                text=True,
                check=True
            )
            output = result.stdout + result.stderr
            return CherryPickResult(
                success=True,
                has_conflicts=False,
                is_empty=False,
                raw_output=output
            )
        except subprocess.CalledProcessError as e:
            output = (e.stdout or '') + (e.stderr or '')
            output_lower = output.lower()
            
            # "nothing to commit" или "empty" - не ошибка, просто пустой коммит
            is_empty = "nothing to commit" in output_lower or "empty" in output_lower
            has_conflicts = "conflict" in output_lower
            
            if is_empty:
                # НЕ делаем --skip, просто возвращаем результат
                return CherryPickResult(
                    success=True,  # Считаем успехом
                    has_conflicts=False,
                    is_empty=True,
                    raw_output=output
                )
            
            if has_conflicts:
                return CherryPickResult(
                    success=False,
                    has_conflicts=True,
                    is_empty=False,
                    raw_output=output
                )
            
            # Другая ошибка - пробрасываем дальше
            raise
    
    def add_all(self):
        """git add -A"""
        self.git_run("add", "-A")
    
    def commit(self, message: str):
        """git commit"""
        self.git_run("commit", "-m", message)
    
    def push(self, branch: str, set_upstream: bool = True):
        """Push ветки"""
        if set_upstream:
            self.git_run("push", "--set-upstream", "origin", branch)
        else:
            self.git_run("push", "origin", branch)
    
    def cherry_pick_abort(self):
        """Abort cherry-pick"""
        try:
            self.git_run("cherry-pick", "--abort")
        except subprocess.CalledProcessError:
            # Может быть не в состоянии cherry-pick
            pass


class ConflictHandler:
    """Обработка конфликтов при cherry-pick"""
    
    CONFLICT_STATUS_CODES = ['UU', 'AA', 'DD', 'DU', 'UD', 'AU', 'UA']
    
    def __init__(self, git_repo: GitRepository, logger):
        self.git_repo = git_repo
        self.logger = logger
    
    def extract_conflict_messages(self, git_output: str) -> List[str]:
        """Извлекает CONFLICT сообщения из git вывода"""
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
                    if next_line.startswith('Version') or next_line:
                        conflict_msg += ' ' + next_line
                    j += 1
                conflict_messages.append(conflict_msg)
        
        return conflict_messages
    
    def find_conflict_message_for_file(self, file_path: str, conflict_messages: List[str]) -> Optional[str]:
        """Находит сообщение о конфликте для файла"""
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
        """Находит первую строку с конфликтом"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, start=1):
                    if line.strip().startswith('<<<<<<<'):
                        return line_num
        except (FileNotFoundError, IOError, UnicodeDecodeError) as e:
            self.logger.debug(f"Failed to read file {file_path} to find conflict line: {e}")
        return None
    
    def detect_conflicts(self, git_output: str) -> List[ConflictInfo]:
        """Обнаруживает конфликты из git status и вывода"""
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
                
                # Если не нашли через status, пробуем git diff
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
        """Коммитит конфликты для ручного разрешения"""
        try:
            self.git_repo.add_all()
            self.git_repo.commit(f"BACKPORT-CONFLICT: manual resolution required for commit {commit_sha[:7]}")
            self.logger.info(f"Conflict committed for commit {commit_sha[:7]}")
            return True
        except Exception as e:
            self.logger.error(f"Error committing conflicts for commit {commit_sha[:7]}: {e}")
            return False


class GitHubClient:
    """Обертка над GitHub API"""
    
    def __init__(self, repo, token, logger):
        self.repo = repo
        self.token = token
        self.logger = logger
    
    def get_linked_issues_graphql(self, pr_number: int) -> List[str]:
        """Получает связанные issues через GraphQL API"""
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
        """Получает связанные issues для всех PR"""
        all_issues = []
        
        for pull in pull_requests:
            issues = self.get_linked_issues_graphql(pull.number)
            
            # Если GraphQL не вернул issues, парсим из PR body
            if not issues and pull.body:
                body_issues = re.findall(r'#(\d+)', pull.body)
                issues = [f"#{num}" for num in body_issues]
            
            all_issues.extend(issues)
        
        # Убираем дубликаты
        unique_issues = list(dict.fromkeys(all_issues))
        return ' '.join(unique_issues) if unique_issues else 'None'
    
    def create_pr(self, base: str, head: str, title: str, body: str, draft: bool = False):
        """Создает PR"""
        return self.repo.create_pull(
            base=base,
            head=head,
            title=title,
            body=body,
            maintainer_can_modify=True,
            draft=draft
        )


class PRContentBuilder:
    """Генерация контента для PR (title, body)"""
    
    def __init__(self, repo_name: str, github_client: GitHubClient, logger):
        self.repo_name = repo_name
        self.github_client = github_client
        self.logger = logger
    
    def extract_changelog_category(self, pr_body: str) -> Optional[str]:
        """Извлекает Changelog category из PR body"""
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
        """Извлекает Changelog entry из PR body"""
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
        """Извлекает только содержимое Changelog entry"""
        return self.extract_changelog_entry(pr_body, stop_at_category=True)
    
    def get_changelog_info(self, pull_requests: List) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Получает Changelog информацию из всех PR"""
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
        """Генерирует title с CONFLICT если есть конфликты"""
        if len(source_info.titles) == 1:
            base_title = f"[Backport {target_branch}] {source_info.titles[0]}"
        else:
            base_title = f"[Backport {target_branch}] cherry-pick {', '.join(source_info.titles)}"
        
        # Добавляем CONFLICT в начало если есть конфликты
        if has_conflicts:
            base_title = f"[CONFLICT] {base_title}"
        
        # Обрезаем длинные title
        if len(base_title) > 200:
            base_title = base_title[:197] + "..."
        
        return base_title
    
    def build_body(self, context: PRContext) -> str:
        """Генерирует PR body с сырым логом cherry-pick"""
        # Получаем issues
        issue_refs = self.github_client.get_linked_issues(context.source_prs)
        
        # Форматируем авторов
        authors = ', '.join([f"@{author}" for author in set(context.authors)]) if context.authors else "Unknown"
        
        # Получаем changelog информацию
        changelog_category, changelog_entry_text, merged_entry_content = self.get_changelog_info(context.source_prs)
        context.changelog_category = changelog_category
        
        # Определяем changelog entry
        branch_desc = context.target_branch
        if merged_entry_content:
            changelog_entry = merged_entry_content
        elif not changelog_entry_text:
            if len(context.source_prs) == 1:
                pr_num = context.source_prs[0].number
                changelog_entry = f"Backport of PR #{pr_num} to `{branch_desc}`"
            else:
                pr_nums = ', '.join([f"#{p.number}" for p in context.source_prs])
                changelog_entry = f"Backport of PRs {pr_nums} to `{branch_desc}`"
        else:
            changelog_entry = changelog_entry_text
        
        # Убеждаемся что entry минимум 20 символов
        if len(changelog_entry) < 20:
            changelog_entry = f"Backport to `{branch_desc}`: {changelog_entry}"
        
        # Для Bugfix категории добавляем issue reference
        if changelog_category and changelog_category.startswith("Bugfix") and issue_refs and issue_refs != "None":
            has_issue = any(re.search(pattern, changelog_entry) for pattern in ISSUE_PATTERNS)
            if not has_issue:
                changelog_entry = f"{changelog_entry} ({issue_refs})"
        
        context.changelog_entry = changelog_entry
        
        # Строим category section
        if changelog_category:
            category_section = f"* {changelog_category}"
        else:
            category_section = get_category_section_template()
        
        # Строим description section
        commits = ""
        if context.source_info and context.source_info.body_items:
            commits = '\n'.join([f"* {item}" for item in context.source_info.body_items])
        elif context.source_prs:
            commits = '\n'.join([f"* PR {pr.html_url}" for pr in context.source_prs])
        
        description_section = f"#### Original PR(s)\n{commits}\n\n"
        description_section += f"#### Metadata\n"
        description_section += f"- **Original PR author(s):** {authors}\n"
        description_section += f"- **Cherry-picked by:** @{context.workflow_triggerer}\n"
        description_section += f"- **Related issues:** {issue_refs}"
        
        # Добавляем сырой лог git cherry-pick (если есть)
        cherry_pick_log_section = ""
        if context.cherry_pick_logs:
            cherry_pick_log_section = "\n\n### Git Cherry-Pick Log\n\n"
            cherry_pick_log_section += "```\n"
            for log in context.cherry_pick_logs:
                cherry_pick_log_section += log
                if not log.endswith('\n'):
                    cherry_pick_log_section += '\n'
            cherry_pick_log_section += "```\n"
        
        # Добавляем conflicts section (если есть)
        conflicts_section = ""
        if context.has_conflicts:
            branch_for_instructions = context.dev_branch_name or context.target_branch
            conflicts_section = "\n\n#### Conflicts Require Manual Resolution\n\n"
            conflicts_section += "This PR contains merge conflicts that require manual resolution.\n\n"
            
            if context.conflict_files:
                conflicts_section += "**Files with conflicts:**\n\n"
                for conflict in context.conflict_files:
                    # Простая ссылка на файлы в PR (без diff hash)
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

После разрешения конфликтов:
1. Поправь title PR (убери `[CONFLICT]` если конфликты разрешены)
2. Отметь PR как ready for review
"""
        
        # Добавляем workflow link
        workflow_section = ""
        if context.workflow_url:
            workflow_section = f"\n\n---\n\nPR was created by cherry-pick workflow [run]({context.workflow_url})"
        else:
            workflow_section = "\n\n---\n\nPR was created by cherry-pick script"
        
        # Собираем полный body
        pr_body = f"""### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

{changelog_entry}

### Changelog category <!-- remove all except one -->

{category_section}

### Description for reviewers <!-- (optional) description for those who read this PR -->

{description_section}{cherry_pick_log_section}{conflicts_section}{workflow_section}
"""
        
        return pr_body


class CommentManager:
    """Управление комментариями в оригинальных PR"""
    
    def __init__(self, logger):
        self.logger = logger
        self.backport_comments = []  # [(pull, comment), ...]
    
    def find_existing_backport_comment(self, pull):
        """Находит существующий backport комментарий"""
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
        """Создает или обновляет начальный комментарий о начале backport"""
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
        """Обновляет комментарии с результатами backport"""
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
                    status = "draft PR" if result.has_conflicts else "PR"
                    new_results = f"Backported to `{result.target_branch}`: {status} {result.pr.html_url}"
                    if result.has_conflicts:
                        new_results += " (contains conflicts requiring manual resolution)"
                    if workflow_url:
                        new_results += f" - [workflow run]({workflow_url})"
                else:
                    new_results = "Backport results:\n"
                    for result in results:
                        status = "draft PR" if result.has_conflicts else "PR"
                        conflict_note = " (contains conflicts requiring manual resolution)" if result.has_conflicts else ""
                        new_results += f"- `{result.target_branch}`: {status} {result.pr.html_url}{conflict_note}\n"
                    for target_branch, reason in skipped_branches:
                        new_results += f"- `{target_branch}`: skipped ({reason})\n"
                    if workflow_url:
                        new_results += f"\n[workflow run]({workflow_url})"
                
                # Заменяем "in progress" строку
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
    """Запись в GitHub Actions summary"""
    
    def __init__(self, logger):
        self.logger = logger
        self.summary_path = os.getenv('GITHUB_STEP_SUMMARY')
    
    def write(self, msg: str):
        """Записывает сообщение в summary и логирует"""
        self.logger.info(msg)
        if self.summary_path:
            with open(self.summary_path, 'a') as summary:
                summary.write(f'{msg}\n\n')
    
    def write_branch_result(self, branch: str, pr, has_conflicts: bool, conflict_files: List[ConflictInfo], cherry_pick_logs: List[str]):
        """Записывает результат для ветки с сырым логом"""
        summary = f"### Branch `{branch}`: "
        if has_conflicts:
            summary += f"**CONFLICT** Draft PR {pr.html_url}\n\n"
        else:
            summary += f"PR {pr.html_url}\n\n"
        
        # Добавляем сырой лог
        if cherry_pick_logs:
            summary += "**Git Cherry-Pick Log:**\n\n"
            summary += "```\n"
            for log in cherry_pick_logs:
                summary += log
            summary += "```\n\n"
        
        # Добавляем конфликты (если есть)
        if has_conflicts and conflict_files:
            summary += "**Files with conflicts:**\n\n"
            for conflict in conflict_files:
                summary += f"- `{conflict.file_path}`\n"
                if conflict.message:
                    summary += f"  - `{conflict.message}`\n"
        
        self.write(summary)


class InputParser:
    """Парсинг и нормализация входных данных"""
    
    @staticmethod
    def split(s: str, seps: str = ', \n') -> List[str]:
        """Разделяет строку по нескольким разделителям"""
        if not s:
            return []
        if not seps:
            return [s]
        result = []
        for part in s.split(seps[0]):
            result += InputParser.split(part, seps[1:])
        return result
    
    @staticmethod
    def normalize_commit_ref(ref: str) -> str:
        """Нормализует commit reference (убирает URL, оставляет только ID)"""
        return ref.split('/')[-1].strip()


class InputValidator:
    """Валидация входных данных"""
    
    def __init__(self, repo, allow_unmerged: bool, logger):
        self.repo = repo
        self.allow_unmerged = allow_unmerged
        self.logger = logger
    
    def validate_prs(self, pull_requests: List):
        """Валидирует PR"""
        for pull in pull_requests:
            try:
                if not hasattr(pull, "merged"):
                    raise ValueError(f"PR #{pull.number} object is missing 'merged' attribute, cannot validate")
                if not pull.merged and not self.allow_unmerged:
                    raise ValueError(f"PR #{pull.number} is not merged. Use --allow-unmerged to allow backporting unmerged PRs")
            except GithubException as e:
                raise ValueError(f"PR #{pull.number} does not exist or is not accessible: {e}")
    
    def validate_commits(self, commit_shas: List[str]):
        """Валидирует commits"""
        for commit_sha in commit_shas:
            try:
                commit = self.repo.get_commit(commit_sha)
                if not commit.sha:
                    raise ValueError(f"Commit {commit_sha} not found")
            except GithubException as e:
                raise ValueError(f"Commit {commit_sha} does not exist: {e}")
    
    def validate_branches(self, branches: List[str]):
        """Валидирует ветки"""
        for branch in branches:
            try:
                self.repo.get_branch(branch)
            except GithubException as e:
                raise ValueError(f"Branch {branch} does not exist: {e}")
    
    def validate_all(self, commit_shas: List[str], branches: List[str], pull_requests: List):
        """Валидирует все входные данные"""
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
    """Основной класс для координации cherry-pick процесса с новой архитектурой"""
    
    def __init__(self, args, repo_name: str, token: str, workflow_triggerer: str, workflow_url: Optional[str] = None):
        self.logger = logging.getLogger("cherry-pick")
        self.repo_name = repo_name
        self.token = token
        self.workflow_triggerer = workflow_triggerer
        self.workflow_url = workflow_url
        
        # Инициализация компонентов
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
        
        # Параметры
        self.merge_commits_mode = getattr(args, 'merge_commits', 'skip')
        self.allow_unmerged = getattr(args, 'allow_unmerged', False)
        
        # Парсинг входных данных
        commits_str = args.commits
        branches_str = args.target_branches
        
        commits = InputParser.split(commits_str)
        self.target_branches = InputParser.split(branches_str)
        
        # Собираем информацию об исходных PR/commits
        self.source_info = SourceInfo()
        self.pull_requests = []
        
        for c in commits:
            ref = InputParser.normalize_commit_ref(c)
            try:
                pr_num = int(ref)
                self._add_pull(pr_num, len(commits) == 1)
            except ValueError:
                self._add_commit(ref, len(commits) == 1)
        
        # Результаты
        self.results: List[BackportResult] = []
        self.skipped_branches: List[Tuple[str, str]] = []
        self.has_errors = False
        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M%S")
    
    def _add_commit(self, c: str, single: bool):
        """Добавляет commit по SHA"""
        try:
            expanded_sha = self.commit_resolver.expand_sha(c)
        except ValueError as e:
            self.logger.error(f"Failed to expand SHA {c}: {e}")
            raise
        
        commit = self.repo.get_commit(expanded_sha)
        
        # Проверка merge commit
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
            if not pr.merged:
                if not self.allow_unmerged:
                    raise ValueError(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged. Cannot backport unmerged PR. Use --allow-unmerged to allow")
                else:
                    self.logger.info(f"PR #{pr.number} (associated with commit {expanded_sha[:7]}) is not merged, but --allow-unmerged is set, proceeding")
            
            existing_pr_numbers = [p.number for p in self.pull_requests]
            if pr.number not in existing_pr_numbers:
                self.pull_requests.append(pr)
                self.source_info.pull_requests.append(pr)
                self.source_info.authors.append(pr.user.login)
            
            if single:
                self.source_info.titles.append(pr.title)
            else:
                self.source_info.titles.append(f'commit {commit.sha[:7]}')
            self.source_info.body_items.append(f"* commit {commit.html_url}: {pr.title}")
        else:
            try:
                commit_author = commit.author.login if commit.author else None
                if commit_author and commit_author not in self.source_info.authors:
                    self.source_info.authors.append(commit_author)
            except AttributeError:
                pass
            
            if single:
                self.source_info.titles.append(f'cherry-pick commit {commit.sha[:7]}')
            else:
                self.source_info.titles.append(f'commit {commit.sha[:7]}')
            self.source_info.body_items.append(f"* commit {commit.html_url}")
        
        self.source_info.commit_shas.append(commit.sha)
    
    def _get_commits_from_pr(self, pull) -> List[str]:
        """Получает список commits из PR (исключая merge commits)"""
        commits = []
        for commit in pull.get_commits():
            commit_obj = commit.commit
            if self.merge_commits_mode == 'skip':
                if commit_obj.parents and len(commit_obj.parents) > 1:
                    self.logger.info(f"Skipping merge commit {commit.sha[:7]} from PR #{pull.number}")
                    continue
            commits.append(commit.sha)
        return commits
    
    def _add_pull(self, p: int, single: bool):
        """Добавляет PR и получает commits из него"""
        pull = self.repo.get_pull(p)
        
        if not pull.merged:
            if not self.allow_unmerged:
                raise ValueError(f"PR #{p} is not merged. Use --allow-unmerged to backport unmerged PRs")
            else:
                self.logger.info(f"PR #{p} is not merged, but --allow-unmerged is set, proceeding with commits from PR")
        
        self.pull_requests.append(pull)
        self.source_info.pull_requests.append(pull)
        self.source_info.authors.append(pull.user.login)
        
        if single:
            self.source_info.titles.append(f"{pull.title}")
        else:
            self.source_info.titles.append(f'PR {pull.number}')
        self.source_info.body_items.append(f"* PR {pull.html_url}")
        
        pr_commits = self._get_commits_from_pr(pull)
        if not pr_commits:
            raise ValueError(f"PR #{p} contains no commits to cherry-pick")
        
        if not pull.merged:
            self.source_info.commit_shas.extend(pr_commits)
            self.logger.info(f"PR #{p} is unmerged, using {len(pr_commits)} commits from PR")
        elif pull.merge_commit_sha:
            merge_commit = self.repo.get_commit(pull.merge_commit_sha)
            if merge_commit.parents and len(merge_commit.parents) > 1:
                self.source_info.commit_shas.extend(pr_commits)
                self.logger.info(f"PR #{p} was merged as merge commit, using {len(pr_commits)} individual commits")
            else:
                self.source_info.commit_shas.append(pull.merge_commit_sha)
                self.logger.info(f"PR #{p} was merged as squash/rebase, using merge_commit_sha")
        else:
            self.source_info.commit_shas.extend(pr_commits)
    
    def process(self):
        """Основной метод обработки"""
        # Валидация
        try:
            self.validator.validate_all(
                self.source_info.commit_shas,
                self.target_branches,
                self.pull_requests
            )
        except ValueError as e:
            error_msg = f"VALIDATION_ERROR: {e}"
            self.logger.error(error_msg)
            self.summary_writer.write(error_msg)
            sys.exit(1)
        
        # Создание начального комментария
        self.comment_manager.create_initial_comment(
            self.pull_requests,
            self.target_branches,
            self.workflow_url
        )
        
        # Клонирование репозитория
        try:
            repo_url = f"https://{self.token}@github.com/{self.repo_name}.git"
            self.git_repo.clone(repo_url, "ydb-new-pr")
        except subprocess.CalledProcessError as e:
            error_msg = f"REPOSITORY_CLONE_ERROR: Failed to clone repository {self.repo_name}: {e}"
            self.logger.error(error_msg)
            self.summary_writer.write(error_msg)
            sys.exit(1)
        
        os.chdir("ydb-new-pr")
        
        # Обработка каждой целевой ветки
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
        
        # Обновление комментариев
        self.comment_manager.update_with_results(
            self.results,
            self.skipped_branches,
            self.target_branches,
            self.workflow_url
        )
        
        # Проверка ошибок
        if self.has_errors:
            error_msg = "WORKFLOW_FAILED: Cherry-pick workflow completed with errors. Check logs above for details."
            self.logger.error(error_msg)
            self.summary_writer.write(error_msg)
            sys.exit(1)
        
        self.logger.info("WORKFLOW_SUCCESS: All cherry-pick operations completed successfully")
        self.summary_writer.write("All cherry-pick operations completed successfully")
    
    def _process_branch(self, target_branch: str) -> BackportResult:
        """Обрабатывает одну ветку"""
        dev_branch_name = f"cherry-pick-{target_branch}-{self.dtm}"
        all_conflict_files: List[ConflictInfo] = []
        cherry_pick_logs: List[str] = []
        
        # Подготовка ветки
        self.git_repo.fetch("origin", target_branch)
        self.git_repo.reset_hard()
        self.git_repo.checkout_branch(target_branch, f"origin/{target_branch}")
        self.git_repo.create_branch(dev_branch_name, target_branch)
        
        # Cherry-pick каждого коммита
        for commit_sha in self.source_info.commit_shas:
            result = self.git_repo.cherry_pick(commit_sha)
            
            # Сохраняем сырой лог
            if result.raw_output:
                cherry_pick_logs.append(f"=== Cherry-picking {commit_sha[:7]} ===\n{result.raw_output}")
            
            # Обработка пустых коммитов - просто логируем, не пропускаем
            if result.is_empty:
                self.logger.info(f"Commit {commit_sha[:7]} is empty (already applied), continuing...")
                continue  # Продолжаем, не делаем skip
            
            # Обработка конфликтов
            if result.has_conflicts:
                conflicts = self.conflict_handler.detect_conflicts(result.raw_output)
                if conflicts:
                    if self.conflict_handler.commit_conflicts(commit_sha, conflicts):
                        all_conflict_files.extend(conflicts)
                    else:
                        # Не удалось закоммитить конфликты
                        self.git_repo.cherry_pick_abort()
                        self.has_errors = True
                        error_msg = f"CHERRY_PICK_CONFLICT_ERROR: Failed to handle conflict for commit {commit_sha[:7]} in branch {target_branch}"
                        self.logger.error(error_msg)
                        self.summary_writer.write(error_msg)
                        self.skipped_branches.append((target_branch, "cherry-pick conflict (failed to resolve)"))
                        raise Exception(error_msg)
        
        # Push ветки
        try:
            self.git_repo.push(dev_branch_name, set_upstream=True)
        except subprocess.CalledProcessError:
            self.has_errors = True
            raise
        
        # Создание PR
        has_conflicts = len(all_conflict_files) > 0
        
        # Создаем контекст для PR
        context = PRContext(
            target_branch=target_branch,
            dev_branch_name=dev_branch_name,
            repo_name=self.repo_name,
            source_prs=self.pull_requests,
            source_info=self.source_info,
            authors=self.source_info.authors,
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
            
            # Обновляем body с PR номером для правильных ссылок
            if has_conflicts:
                updated_body = self.pr_builder.build_body(context)
                pr.edit(body=updated_body)
            
            # Назначаем assignee
            if self.workflow_triggerer and self.workflow_triggerer != 'unknown':
                try:
                    pr.add_to_assignees(self.workflow_triggerer)
                except GithubException:
                    pass
            
            # Включаем automerge если нет конфликтов
            if not has_conflicts:
                try:
                    pr.enable_automerge(merge_method='MERGE')
                except Exception:
                    try:
                        pr.enable_automerge(merge_method='SQUASH')
                    except Exception:
                        pass
            
            # Записываем в summary
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
                has_conflicts=has_conflicts,
                conflict_files=all_conflict_files,
                cherry_pick_logs=cherry_pick_logs
            )
        except GithubException as e:
            self.has_errors = True
            self.logger.error(f"PR_CREATION_ERROR: Failed to create PR for branch {target_branch}: {e}")
            raise


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
        self.created_backport_prs = []  # Store created backport PRs: [(target_branch, pr, has_conflicts, conflict_files), ...]
        self.skipped_branches = []  # Store branches where PR was not created: [(target_branch, reason), ...]
        self.backport_comments = []  # Store comment objects for editing: [(pull, comment), ...]
        
        # Use datetime with seconds to avoid branch name conflicts
        self.dtm = datetime.datetime.now().strftime("%y%m%d-%H%M%S")
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
        # Note: This requires the repository to be cloned first (done in process() method)
        try:
            result = subprocess.run(
                ['git', 'rev-parse', short_sha],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            # Repository not cloned yet or git command failed
            raise ValueError(f"Failed to expand SHA {short_sha} via git rev-parse. Repository must be cloned first. Error: {e}")

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
            except AttributeError:
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
        # Validate PRs from self.pull_requests (already loaded, no need to re-fetch)
        for pull in self.pull_requests:
            try:
                # Use already loaded PR object instead of re-fetching
                if not hasattr(pull, "merged"):
                    raise ValueError(f"PR #{pull.number} object is missing 'merged' attribute, cannot validate")
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
            cat_clean = cat.strip()
            if cat_clean:
                # Return the category as-is, without hardcoded validation
                return cat_clean
        
        return None

    def _extract_changelog_entry(self, pr_body: str, stop_at_category: bool = False) -> Optional[str]:
        """Extract Changelog entry from PR body
        
        Args:
            pr_body: PR body text
            stop_at_category: If True, stop at "### Changelog category", otherwise stop at any "###"
        
        Returns:
            Content of Changelog entry section, or None if not found
        """
        if not pr_body:
            return None
        
        # Choose regex pattern based on stop_at_category
        if stop_at_category:
            pattern = r"### Changelog entry.*?\n(.*?)(\n### Changelog category|$)"
        else:
            pattern = r"### Changelog entry.*?\n(.*?)(\n###|$)"
        
        entry_match = re.search(pattern, pr_body, re.DOTALL)
        if not entry_match:
            return None
        
        entry = entry_match.group(1).strip()
        # Skip if it's just "..." or empty
        if entry in ['...', '']:
            return None
        
        return entry

    def _extract_changelog_entry_content(self, pr_body: str) -> Optional[str]:
        """Extract only the content inside Changelog entry section (without header and content before it)
        
        Returns only the text content inside "### Changelog entry" section (up to "### Changelog category").
        """
        return self._extract_changelog_entry(pr_body, stop_at_category=True)

    def _get_changelog_info(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Get Changelog category, entry, and merged entry content from original PRs
        
        For multiple PRs: merges Changelog entry contents with '---' separator.
        If categories differ between PRs, returns None for category (to use template).
        Returns: (category, entry_text, merged_entry_content)
        """
        categories = []  # Collect all categories from all PRs
        entry = None
        entry_contents = []  # Collect all Changelog entry contents to merge
        
        # Collect info from all PRs
        for pull in self.pull_requests:
            if pull.body:
                # Extract category from each PR
                cat = self._extract_changelog_category(pull.body)
                if cat:
                    categories.append(cat)
                
                # Extract entry text (take first non-empty) - used for building new entry if merged content not available
                if not entry:
                    ent = self._extract_changelog_entry(pull.body)
                    if ent:
                        entry = ent
                
                # Extract only Changelog entry content (without header)
                entry_content = self._extract_changelog_entry_content(pull.body)
                if entry_content:
                    entry_contents.append(entry_content)
        
        # Determine category: use it only if all PRs have the same category
        category = None
        if categories:
            # Check if all categories are the same
            unique_categories = set(categories)
            if len(unique_categories) == 1:
                # All PRs have the same category, use it
                category = categories[0]
            # If categories differ, category remains None (will use template)
        
        # Merge entry contents if multiple PRs have content
        if len(entry_contents) > 1:
            # For multiple PRs, combine with separator
            merged_entry_content = "\n\n---\n\n".join(entry_contents)
        elif len(entry_contents) == 1:
            merged_entry_content = entry_contents[0]
        else:
            merged_entry_content = None
        
        return category, entry, merged_entry_content

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

    def pr_body(self, with_wf: bool, has_conflicts: bool = False, target_branch: Optional[str] = None, dev_branch_name: Optional[str] = None, conflict_files: Optional[List[str]] = None, pr_number: Optional[int] = None) -> str:
        """Generate PR body with improved format that passes validation"""
        commits = '\n'.join(self.pr_body_list)
        
        # Get linked issues
        issue_refs = self._get_linked_issues()
        
        # Format author information
        authors = ', '.join([f"@{author}" for author in set(self.pull_authors)]) if self.pull_authors else "Unknown"
        
        # Determine target branch for description
        branch_desc = target_branch if target_branch else ', '.join(self.target_branches)
        
        # Get Changelog category, entry, and merged entry content from original PRs
        changelog_category, changelog_entry_text, merged_entry_content = self._get_changelog_info()
        
        # Use merged entry content if available, otherwise build entry
        if merged_entry_content:
            # Use merged content from original PRs directly in Changelog entry
            changelog_entry = merged_entry_content
        elif not changelog_entry_text:
            # Build changelog entry if not found
            if len(self.pull_requests) == 1:
                pr_num = self.pull_requests[0].number
                changelog_entry = f"Backport of PR #{pr_num} to `{branch_desc}`"
            else:
                pr_nums = ', '.join([f"#{p.number}" for p in self.pull_requests])
                changelog_entry = f"Backport of PRs {pr_nums} to `{branch_desc}`"
        else:
            # Use extracted entry text as fallback
            changelog_entry = changelog_entry_text
        
        # Ensure entry is at least 20 characters (validation requirement)
        if len(changelog_entry) < 20:
            changelog_entry = f"Backport to `{branch_desc}`: {changelog_entry}"
        
        # For Bugfix category, ensure there's an issue reference in changelog entry (validation requirement)
        if changelog_category and changelog_category.startswith("Bugfix") and issue_refs and issue_refs != "None":
            # Check if issue reference is already in entry
            has_issue = any(re.search(pattern, changelog_entry) for pattern in ISSUE_PATTERNS)
            if not has_issue:
                # Add issue reference to entry
                changelog_entry = f"{changelog_entry} ({issue_refs})"
        
        # Build category section - use template if category not found
        if changelog_category:
            category_section = f"* {changelog_category}"
        else:
            # Use full template if category not found - let user choose
            category_section = get_category_section_template()
        
        # Build description for reviewers section
        description_section = f"#### Original PR(s)\n{commits}\n\n"
        description_section += f"#### Metadata\n"
        description_section += f"- **Original PR author(s):** {authors}\n"
        description_section += f"- **Cherry-picked by:** @{self.workflow_triggerer}\n"
        description_section += f"- **Related issues:** {issue_refs}"
        
        # Add conflicts section if needed
        if has_conflicts:
            # Use dev_branch_name if provided, otherwise use target_branch as fallback
            branch_for_instructions = dev_branch_name if dev_branch_name else (target_branch if target_branch else "<branch_name>")
            
            # Build conflicted files list with links and conflict messages
            conflicted_files_section = ""
            if conflict_files:
                conflicted_files_section = "\n\n**Files with conflicts:**\n"
                for conflict_item in conflict_files:
                    # Handle both tuple (file_path, line, message) and string (file_path) for backward compatibility
                    if isinstance(conflict_item, tuple):
                        file_path = conflict_item[0]
                        conflict_line = conflict_item[1] if len(conflict_item) > 1 else None
                        conflict_message = conflict_item[2] if len(conflict_item) > 2 else None
                    else:
                        file_path = conflict_item
                        conflict_line = None
                        conflict_message = None
                    
                    # Create link to file in PR diff (if PR is created) or branch (as fallback)
                    if pr_number:
                        # Link to file in PR diff - GitHub will show conflicts
                        # Format: https://github.com/{repo}/pull/{pr_number}/files#diff-{hash}R{line}
                        # GitHub diff hash is SHA256 of file content in hex format (64 chars)
                        # We need to get it from PR files API
                        diff_hash = self._get_file_diff_hash(file_path, pr_number)
                        if diff_hash:
                            file_link = f"https://github.com/{self.repo_name}/pull/{pr_number}/files#diff-{diff_hash}"
                            if conflict_line:
                                file_link += f"R{conflict_line}"
                        else:
                            # Fallback: use simple link to PR files page (no anchor, since we can't get the hash)
                            # GitHub doesn't support encoded path in diff anchor, so we link to files tab without anchor
                            file_link = f"https://github.com/{self.repo_name}/pull/{pr_number}/files"
                            # Line number is shown in link text, not in URL
                    else:
                        # Fallback: link to file in branch (will be updated after PR creation)
                        file_link = f"https://github.com/{self.repo_name}/blob/{branch_for_instructions}/{file_path}"
                        if conflict_line:
                            file_link += f"#L{conflict_line}"
                    
                    # Add line number to display text if available (always try to show it if we have it)
                    # If conflict_line is None, it means the whole file is in conflict, so don't add line number
                    display_text = f"{file_path} (line {conflict_line})" if conflict_line else file_path
                    conflicted_files_section += f"- [{display_text}]({file_link})\n"
                    
                    # Add conflict message if available
                    if conflict_message:
                        conflicted_files_section += f"  - `{conflict_message}`\n"
            
            description_section += f"""
#### Conflicts Require Manual Resolution

This PR contains merge conflicts that require manual resolution.{conflicted_files_section}

**How to resolve conflicts:**

```bash
git fetch origin
git checkout --track origin/{branch_for_instructions}
# Resolve conflicts in files
git add .
git commit -m "Resolved merge conflicts"
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
    
    def _compute_file_hash(self, base64_content: str) -> str:
        """Compute SHA256 hash from base64-encoded file content
        
        Hashes bytes directly to support both text and binary files correctly.
        """
        decoded_bytes = base64.b64decode(base64_content)
        return hashlib.sha256(decoded_bytes).hexdigest()
    
    def _get_file_diff_hash(self, file_path: str, pr_number: int) -> Optional[str]:
        """Get diff hash for GitHub PR file link using GitHub API
        
        GitHub uses SHA256 hash of file content in hex format (64 chars) for diff anchors.
        Format: #diff-{64-char-hex-sha256}R{line}
        We fetch file content from the PR's head branch and compute SHA256 hash.
        """
        try:
            pr = self.repo.get_pull(pr_number)
            file_content = self.repo.get_contents(file_path, ref=pr.head.sha)
            if file_content and hasattr(file_content, 'content'):
                return self._compute_file_hash(file_content.content)
        except (GithubException, Exception) as e:
            self.logger.debug(f"Failed to get diff hash for {file_path} from PR #{pr_number}: {e}")
        
        return None
    
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

    def _extract_conflict_messages(self, git_output: str) -> List[str]:
        """Extract CONFLICT messages from git cherry-pick output
        
        Git supports the following conflict types:
        - CONFLICT (content): Both branches modified the same part of a file (status: UU)
        - CONFLICT (modify/delete): File modified in one branch, deleted in another (status: DU or UD)
        - CONFLICT (add/add): File added in both branches with different content (status: AA)
        - CONFLICT (rename/delete): File renamed in one branch, deleted in another
        - CONFLICT (rename/rename): File renamed differently in both branches
        - CONFLICT (delete/modify): File deleted in one branch, modified in another (status: DU or UD)
        
        This function extracts all CONFLICT messages from git output, which includes the full description
        of what happened (e.g., "CONFLICT (modify/delete): file.py deleted in HEAD and modified in...").
        
        Returns:
            List of conflict messages (e.g., "CONFLICT (modify/delete): file.py deleted in HEAD and modified in...")
        """
        conflict_messages = []
        lines = git_output.split('\n')
        
        for i, line in enumerate(lines):
            if 'CONFLICT' in line and '(' in line and ')' in line:
                # Found a CONFLICT line, collect it and any continuation lines
                conflict_msg = line.strip()
                # Check if next line is a continuation (starts with "Version" or is part of the message)
                # Stop if we encounter another CONFLICT, hint:, error:, or empty line
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    # Stop if we encounter another CONFLICT message
                    if 'CONFLICT' in next_line and '(' in next_line and ')' in next_line:
                        break
                    # Stop on hint:, error:, or empty line
                    if next_line.startswith('hint:') or next_line.startswith('error:') or not next_line:
                        break
                    # Continue if it's a continuation (starts with "Version" or is part of the message)
                    if next_line.startswith('Version') or next_line:
                        conflict_msg += ' ' + next_line
                    j += 1
                conflict_messages.append(conflict_msg)
        
        return conflict_messages

    def _find_conflict_message_for_file(self, file_path: str, conflict_messages: List[str]) -> Optional[str]:
        """Find matching conflict message for a file from git output
        
        Args:
            file_path: Path to the conflicted file
            conflict_messages: List of conflict messages from git output
            
        Returns:
            Matching conflict message if found, None otherwise
        """
        file_basename = os.path.basename(file_path)
        for msg in conflict_messages:
            # Git format: "CONFLICT (type): filename ..."
            # Extract filename from message (first word after ": ")
            if ': ' in msg:
                msg_file_part = msg.split(': ', 1)[1]
                # Extract filename (first word after ": ")
                msg_filename = msg_file_part.split()[0] if msg_file_part.split() else ""
                # Match if filename matches (handle both relative and absolute paths)
                if msg_filename == file_path or msg_filename == file_basename:
                    return msg
            # Fallback: check if file_path appears right after "CONFLICT (type): "
            elif file_path in msg:
                # Make sure it's not a substring match (e.g., "file.py" in "file.pyc")
                # Check if file_path appears as a word boundary match
                if re.search(rf'\b{re.escape(file_path)}\b', msg) or re.search(rf'\b{re.escape(file_basename)}\b', msg):
                    return msg
        return None

    def _handle_cherry_pick_conflict(self, commit_sha: str, git_output: str = ""):
        """Handle cherry-pick conflict: commit the conflict and return list of conflicted files with conflict messages
        
        Returns:
            (success, conflict_files) where conflict_files is list of (file_path, conflict_line, conflict_message) tuples
        """
        conflict_files = []  # List of (file_path, conflict_line, conflict_message) tuples
        conflict_messages = self._extract_conflict_messages(git_output)
        
        try:
            # Check if there are conflicts (files with conflicts or unmerged paths)
            result = subprocess.run(
                ['git', 'status', '--porcelain'],
                capture_output=True,
                text=True,
                check=True
            )
            
            if result.stdout.strip():
                # Check for conflict markers in files
                status_lines = result.stdout.strip().split('\n')
                self.logger.debug(f"Git status output: {result.stdout.strip()}")
                
                for line in status_lines:
                    # Git status codes for conflicts:
                    # UU: both modified (content conflict)
                    # AA: both added (add/add conflict)
                    # DD: both deleted (delete/delete - usually not a conflict, but git marks it)
                    # DU: deleted by us, updated by them (modify/delete or delete/modify)
                    # UD: updated by us, deleted by them (modify/delete or delete/modify)
                    # AU: added by us, updated by them (add/modify conflict)
                    # UA: updated by us, added by them (modify/add conflict)
                    if line.startswith('UU') or line.startswith('AA') or line.startswith('DD') or \
                       line.startswith('DU') or line.startswith('UD') or line.startswith('AU') or line.startswith('UA'):
                        # Unmerged paths - definitely a conflict
                        # Extract filename (after status code)
                        parts = line.split(None, 1)  # Split on whitespace, max 1 split
                        if len(parts) > 1:
                            file_path = parts[1].strip()
                            if file_path:
                                # Find first conflict line in file
                                conflict_line = self._find_first_conflict_line(file_path)
                                
                                # Find matching conflict message from git output
                                conflict_message = self._find_conflict_message_for_file(file_path, conflict_messages)
                                
                                conflict_files.append((file_path, conflict_line, conflict_message))
                
                # If we didn't find unmerged paths, try to find files with conflict markers
                if not conflict_files:
                    # Use git diff to find files with conflict markers
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
                                    # Find first conflict line in file
                                    conflict_line = self._find_first_conflict_line(file_path)
                                    
                                    # Find matching conflict message
                                    conflict_message = self._find_conflict_message_for_file(file_path, conflict_messages)
                                    
                                    conflict_files.append((file_path, conflict_line, conflict_message))
                    except subprocess.CalledProcessError:
                        pass
                
                # Log detailed conflict information
                if conflict_files:
                    self.logger.info(f"Detected {len(conflict_files)} conflict(s) for commit {commit_sha[:7]}:")
                    for file_path, conflict_line, conflict_message in conflict_files:
                        if conflict_message:
                            self.logger.info(f"  {conflict_message}")
                        else:
                            self.logger.info(f"  - {file_path}" + (f" (line {conflict_line})" if conflict_line else ""))
                
                # There are changes (conflicts or other modifications)
                self.git_run("add", "-A")
                self.git_run("commit", "-m", f"BACKPORT-CONFLICT: manual resolution required for commit {commit_sha[:7]}")
                self.logger.info(f"Conflict committed for commit {commit_sha[:7]}")
                return True, conflict_files
            else:
                # Working tree is clean - this shouldn't happen if there's a real conflict
                # If git status is empty, the conflict may have been auto-resolved or something else happened
                # Return False to let the caller handle it (it will check for "nothing to commit" in the output)
                self.logger.warning(f"Working tree is clean after conflict detection for commit {commit_sha[:7]}. "
                                  f"This is unexpected - conflict may have been auto-resolved or state is inconsistent.")
                return False, []
        except Exception as e:
            self.logger.error(f"Error handling conflict for commit {commit_sha[:7]}: {e}")
        
        return False, []
    
    def _find_first_conflict_line(self, file_path: str) -> Optional[int]:
        """Find the first line number with conflict markers in a file
        
        Returns:
            Line number of first conflict marker (<<<<<<<) if found, None otherwise.
            None means either the whole file is in conflict or we couldn't determine the line.
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, start=1):
                    if line.strip().startswith('<<<<<<<'):
                        return line_num
        except (FileNotFoundError, IOError, UnicodeDecodeError) as e:
            self.logger.debug(f"Failed to read file {file_path} to find conflict line: {e}")
        return None

    def _find_existing_backport_comment(self, pull):
        """Find existing backport comment in PR (created by previous workflow run)"""
        try:
            # Get all comments for this PR
            comments = pull.get_issue_comments()
            for comment in comments:
                # Check if comment is from YDBot and contains "Backport" keyword
                if comment.user.login == "YDBot" and "Backport" in comment.body:
                    # Check if it's an initial comment (contains "in progress") or a result comment
                    if "in progress" in comment.body:
                        return comment
        except Exception as e:
            self.logger.debug(f"Failed to find existing comment in PR #{pull.number}: {e}")
        return None

    def _create_initial_backport_comment(self):
        """Create or update initial comment in original PRs about backport start"""
        if not self.workflow_url:
            self.logger.warning("Workflow URL not available, skipping initial comment")
            return
        
        target_branches_str = ', '.join([f"`{b}`" for b in self.target_branches])
        new_line = f"Backport to {target_branches_str} in progress: [workflow run]({self.workflow_url})"
        
        for pull in self.pull_requests:
            try:
                # Try to find existing comment
                existing_comment = self._find_existing_backport_comment(pull)
                if existing_comment:
                    # Check if this workflow's branches are already mentioned
                    existing_body = existing_comment.body
                    branches_already_mentioned = all(f"`{b}`" in existing_body for b in self.target_branches)
                    
                    if branches_already_mentioned and self.workflow_url in existing_body:
                        # This workflow run already mentioned, skip update
                        self.backport_comments.append((pull, existing_comment))
                        self.logger.debug(f"Backport comment already contains info for branches {self.target_branches}, skipping update")
                    else:
                        # Append new line to existing comment
                        updated_comment = f"{existing_body}\n\n{new_line}"
                        existing_comment.edit(updated_comment)
                        self.backport_comments.append((pull, existing_comment))
                        self.logger.info(f"Updated existing backport comment in original PR #{pull.number}")
                else:
                    # Create new comment
                    comment = pull.create_issue_comment(new_line)
                    self.backport_comments.append((pull, comment))
                    self.logger.info(f"Created initial backport comment in original PR #{pull.number}")
            except GithubException as e:
                self.logger.warning(f"Failed to create/update initial comment in original PR #{pull.number}: {e}")

    def _update_backport_comment(self):
        """Update comment in original PRs with backport results"""
        if not self.backport_comments:
            return
        
        for pull, comment in self.backport_comments:
            try:
                # Get existing comment body
                existing_body = comment.body
                
                # Build new results section for this workflow run
                total_branches = len(self.created_backport_prs) + len(self.skipped_branches)
                
                if total_branches == 0:
                    # No branches processed (should not happen)
                    new_results = f"Backport to {', '.join([f'`{b}`' for b in self.target_branches])} completed with no results"
                    if self.workflow_url:
                        new_results += f" - [workflow run]({self.workflow_url})"
                elif total_branches == 1 and len(self.created_backport_prs) == 1:
                    # Single branch with PR - simple comment
                    target_branch, pr, has_conflicts, conflict_files = self.created_backport_prs[0]
                    status = "draft PR" if has_conflicts else "PR"
                    new_results = f"Backported to `{target_branch}`: {status} {pr.html_url}"
                    if has_conflicts:
                        new_results += " (contains conflicts requiring manual resolution)"
                    if self.workflow_url:
                        new_results += f" - [workflow run]({self.workflow_url})"
                else:
                    # Multiple branches or mixed results - list all
                    new_results = "Backport results:\n"
                    
                    # List created PRs
                    for target_branch, pr, has_conflicts, conflict_files in self.created_backport_prs:
                        status = "draft PR" if has_conflicts else "PR"
                        conflict_note = " (contains conflicts requiring manual resolution)" if has_conflicts else ""
                        new_results += f"- `{target_branch}`: {status} {pr.html_url}{conflict_note}\n"
                    
                    # List skipped branches
                    for target_branch, reason in self.skipped_branches:
                        new_results += f"- `{target_branch}`: skipped ({reason})\n"
                    
                    if self.workflow_url:
                        new_results += f"\n[workflow run]({self.workflow_url})"
                
                # Replace the "in progress" line for this workflow run, or append results
                if self.workflow_url in existing_body:
                    # Find and replace the "in progress" line for this workflow
                    # Handle both single-line and multi-line results
                    lines = existing_body.split('\n')
                    updated_lines = []
                    workflow_found = False
                    in_results_block = False  # Track if we're inside a multi-line results block
                    
                    for line in lines:
                        if self.workflow_url in line:
                            if "in progress" in line:
                                # Replace "in progress" line with results
                                updated_lines.append(new_results)
                                workflow_found = True
                                # If results are multi-line, mark that we're in a results block
                                if new_results.count('\n') > 0:
                                    in_results_block = True
                            elif in_results_block:
                                # We're inside a results block for this workflow
                                # Skip lines until we find an empty line or a line with different workflow URL
                                if line.strip() == '':
                                    in_results_block = False
                                    updated_lines.append(line)
                                elif self.workflow_url not in line:
                                    # Different workflow or end of results block
                                    in_results_block = False
                                    updated_lines.append(line)
                                # else: skip this line (it's part of old results)
                            else:
                                # Workflow URL found but not "in progress" - might be old results
                                # Check if this line is part of results for this workflow
                                is_result_line = (
                                    any(f"`{b}`" in line for b in self.target_branches) or
                                    "Backport results:" in line or
                                    line.strip().startswith("- `")
                                )
                                if is_result_line and self.workflow_url in line:
                                    # This is a results line for this workflow, replace with new results
                                    updated_lines.append(new_results)
                                    workflow_found = True
                                    if new_results.count('\n') > 0:
                                        in_results_block = True
                                else:
                                    updated_lines.append(line)
                        elif in_results_block:
                            # We're inside a results block, skip until empty line
                            if line.strip() == '':
                                in_results_block = False
                                updated_lines.append(line)
                            # else: skip this line
                        else:
                            updated_lines.append(line)
                    
                    if not workflow_found:
                        # Workflow line not found, append results
                        updated_lines.append("")
                        updated_lines.append(new_results)
                    
                    updated_comment = '\n'.join(updated_lines)
                else:
                    # Workflow URL not in comment, append results
                    updated_comment = f"{existing_body}\n\n{new_results}"
                
                comment.edit(updated_comment)
                self.logger.info(f"Updated backport comment in original PR #{pull.number}")
            except GithubException as e:
                self.logger.warning(f"Failed to update comment in original PR #{pull.number}: {e}")

    def create_pr_for_branch(self, target_branch):
        """Create PR for target branch with conflict handling"""
        dev_branch_name = f"cherry-pick-{target_branch}-{self.dtm}"
        has_conflicts = False
        all_conflict_files = []  # Collect all conflicted files: [(file_path, conflict_line), ...]
        
        # First fetch for up-to-date branch information
        self.git_run("fetch", "origin", target_branch)
        
        self.git_run("reset", "--hard")
        # Create/update local branch based on origin/target_branch (-B overwrites if exists)
        self.git_run("checkout", "-B", target_branch, f"origin/{target_branch}")
        self.git_run("checkout", "-b", dev_branch_name)
        
        # Cherry-pick commits
        # Note: git cherry-pick by default skips empty commits, so we don't need --empty=drop (which doesn't exist)
        for commit_sha in self.commit_shas:
            try:
                self.git_run("cherry-pick", commit_sha)
            except subprocess.CalledProcessError as e:
                error_output = e.output.decode() if e.output else ""
                stderr_output = e.stderr.decode() if e.stderr else ""
                full_output = error_output + stderr_output
                
                # Check if commit is already applied (git cherry-pick returns error but commit is already in branch)
                if "empty" in full_output.lower() or "nothing to commit" in full_output.lower():
                    self.logger.info(f"Commit {commit_sha[:7]} changes is already in branch {target_branch}, skipping")
                    # Quit cherry-pick operation to clean up state before next commit
                    try:
                        self.git_run("cherry-pick", "--skip")
                    except subprocess.CalledProcessError:
                        # If --quit fails, we're probably not in cherry-pick state, continue anyway
                        pass
                    continue
                
                # Check if this is a conflict or another error
                if "conflict" in full_output.lower():
                    self.logger.warning(f"Conflict during cherry-pick of commit {commit_sha[:7]}")
                    conflict_handled, conflict_files = self._handle_cherry_pick_conflict(commit_sha, full_output)
                    if conflict_handled:
                        has_conflicts = True
                        # Add conflicted files to the list (avoid duplicates by file path)
                        # conflict_item is now (file_path, conflict_line, conflict_message) tuple
                        for conflict_item in conflict_files:
                            if len(conflict_item) >= 2:
                                file_path = conflict_item[0]
                                # Check if file_path already exists
                                existing = next((f for f in all_conflict_files if isinstance(f, tuple) and len(f) > 0 and f[0] == file_path), None)
                                if not existing:
                                    all_conflict_files.append(conflict_item)
                                elif len(conflict_item) >= 3 and conflict_item[2] and (len(existing) < 3 or not existing[2]):
                                    # Update with conflict message if we found one and didn't have it before
                                    idx = all_conflict_files.index(existing)
                                    all_conflict_files[idx] = conflict_item
                    else:
                        # Failed to handle conflict, abort cherry-pick
                        self.git_run("cherry-pick", "--abort")
                        self.has_errors = True
                        error_msg = f"CHERRY_PICK_CONFLICT_ERROR: Failed to handle conflict for commit {commit_sha[:7]} in branch {target_branch}"
                        self.logger.error(error_msg)
                        self.add_summary(error_msg)
                        reason = f"cherry-pick conflict (failed to resolve)"
                        self.skipped_branches.append((target_branch, reason))
                        return
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
            # Create PR with initial body (without PR number for diff links)
            pr = self.repo.create_pull(
                base=target_branch,
                head=dev_branch_name,
                title=self.pr_title(target_branch),
                body=self.pr_body(True, has_conflicts, target_branch, dev_branch_name, all_conflict_files if all_conflict_files else None, pr_number=None),
                maintainer_can_modify=True,
                draft=has_conflicts
            )
            
            # If there are conflicts, update PR body with proper diff links
            if has_conflicts and all_conflict_files:
                try:
                    updated_body = self.pr_body(True, has_conflicts, target_branch, dev_branch_name, all_conflict_files, pr_number=pr.number)
                    pr.edit(body=updated_body)
                    self.logger.info(f"Updated PR body with diff links for conflicts")
                except GithubException as e:
                    self.has_errors = True
                    self.logger.error(f"PR_BODY_UPDATE_ERROR: Failed to update PR body with diff links for branch {target_branch}: {e}")
                    raise
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
            summary_msg = f"### Branch `{target_branch}`: Draft PR {pr.html_url} created with conflicts\n\n"
            if all_conflict_files:
                summary_msg += "**Files with conflicts:**\n\n"
                for conflict_item in all_conflict_files:
                    # Handle both tuple (file_path, line, message) and string (file_path) for backward compatibility
                    if isinstance(conflict_item, tuple):
                        file_path = conflict_item[0]
                        conflict_line = conflict_item[1] if len(conflict_item) > 1 else None
                        conflict_message = conflict_item[2] if len(conflict_item) > 2 else None
                    else:
                        file_path = conflict_item
                        conflict_line = None
                        conflict_message = None
                    
                    summary_msg += f"- `{file_path}`"
                    if conflict_line:
                        summary_msg += f" (line {conflict_line})"
                    summary_msg += "\n"
                    
                    if conflict_message:
                        summary_msg += f"  ```\n  {conflict_message}\n  ```\n"
                    summary_msg += "\n"
            self.add_summary(summary_msg)
        else:
            self.logger.info(f"Created PR {pr.html_url} for branch {target_branch}")
            self.add_summary(f"### Branch `{target_branch}`: PR {pr.html_url} created\n")

        # Enable automerge only if there are no conflicts
        if not has_conflicts:
            try:
                pr.enable_automerge(merge_method='MERGE')
            except Exception as e:
                self.logger.warning(f"AUTOMERGE_WARNING: Failed to enable automerge with method MERGE for PR {pr.html_url}: {e}")
                try:
                    pr.enable_automerge(merge_method='SQUASH')
                except Exception as f:
                    self.logger.warning(f"AUTOMERGE_WARNING: Failed to enable automerge with method SQUASH for PR {pr.html_url}: {f}")
        
        # Store created PR for later comment
        self.created_backport_prs.append((target_branch, pr, has_conflicts, all_conflict_files))

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
            except Exception as e:
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
    
    # Используем новый orchestrator
    repo_name = os.environ["REPO"]
    token = os.environ["TOKEN"]
    workflow_triggerer = os.environ.get('GITHUB_ACTOR', 'unknown')
    
    # Получаем workflow URL
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