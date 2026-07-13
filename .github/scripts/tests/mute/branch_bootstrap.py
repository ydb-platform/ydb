"""Bootstrap grace for stable branches newly added to stable_tests_branches.json.

The grace start date is derived from git history (first commit where the branch
appears in the config file). During grace, inherited ``muted_ya`` lines are kept
and are not removed via ``to_delete`` until normal monitor rules apply.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import subprocess
from typing import Optional, Set, Tuple

from mute.constants import get_branch_bootstrap_grace_days

DEFAULT_BRANCHES_CONFIG = '.github/config/stable_tests_branches.json'
_BOOTSTRAP_SKIP_BRANCHES = frozenset({'main'})


def _repo_root() -> str:
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')
    )


def load_inherited_muted_lines(muted_ya_path: str) -> Set[str]:
    lines: Set[str] = set()
    try:
        with open(muted_ya_path, encoding='utf-8') as fp:
            for line in fp:
                line = line.strip()
                if line:
                    lines.add(line)
    except OSError as exc:
        logging.warning('branch bootstrap: cannot read %s: %s', muted_ya_path, exc)
    return lines


def _parse_git_author_date(raw: str) -> Optional[datetime.date]:
    raw = (raw or '').strip()
    if not raw:
        return None
    try:
        if raw.endswith('Z'):
            raw = raw[:-1] + '+00:00'
        dt = datetime.datetime.fromisoformat(raw)
        return dt.astimezone(datetime.timezone.utc).date()
    except ValueError:
        return None


def git_branch_first_seen_in_branches_config(
    branch: str,
    *,
    branches_config_rel: str = DEFAULT_BRANCHES_CONFIG,
    repo_root: Optional[str] = None,
) -> Optional[datetime.date]:
    """Return the author date of the first commit where ``branch`` is in the config."""
    if not branch or branch in _BOOTSTRAP_SKIP_BRANCHES:
        return None

    root = repo_root or _repo_root()
    try:
        proc = subprocess.run(
            ['git', 'log', '--format=%H', '--reverse', '--', branches_config_rel],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        logging.warning('branch bootstrap: git log failed for %s: %s', branch, exc)
        return None

    if proc.returncode != 0:
        logging.warning(
            'branch bootstrap: git log exit %s for %s: %s',
            proc.returncode,
            branches_config_rel,
            (proc.stderr or '').strip(),
        )
        return None

    for commit in (c.strip() for c in proc.stdout.splitlines() if c.strip()):
        show = subprocess.run(
            ['git', 'show', f'{commit}:{branches_config_rel}'],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        if show.returncode != 0:
            continue
        try:
            branches = json.loads(show.stdout)
        except json.JSONDecodeError:
            continue
        if not isinstance(branches, list):
            continue
        names = {str(b).strip() for b in branches if str(b).strip()}
        if branch not in names:
            continue

        date_proc = subprocess.run(
            ['git', 'log', '-1', '--format=%aI', commit],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        if date_proc.returncode != 0:
            continue
        added = _parse_git_author_date(date_proc.stdout)
        if added is not None:
            return added

    return None


def is_branch_bootstrap_grace_active(
    branch: str,
    *,
    today: Optional[datetime.date] = None,
    branches_config_rel: str = DEFAULT_BRANCHES_CONFIG,
    repo_root: Optional[str] = None,
) -> Tuple[bool, Optional[datetime.date]]:
    """Return ``(active, added_date)``; inactive when unknown or grace elapsed."""
    today = today or datetime.datetime.now(datetime.timezone.utc).date()
    added = git_branch_first_seen_in_branches_config(
        branch,
        branches_config_rel=branches_config_rel,
        repo_root=repo_root,
    )
    if added is None:
        return False, None

    grace_days = get_branch_bootstrap_grace_days()
    last_grace_day = added + datetime.timedelta(days=grace_days - 1)
    return today <= last_grace_day, added


def apply_branch_bootstrap_grace(
    *,
    branch: str,
    inherited_muted_ya_path: str,
    all_muted_ya: list[str],
    to_delete: list[str],
    today: Optional[datetime.date] = None,
    branches_config_rel: str = DEFAULT_BRANCHES_CONFIG,
    repo_root: Optional[str] = None,
) -> Tuple[list[str], list[str], bool, Optional[datetime.date]]:
    """Merge inherited mute lines and suppress ``to_delete`` during bootstrap grace."""
    active, added = is_branch_bootstrap_grace_active(
        branch,
        today=today,
        branches_config_rel=branches_config_rel,
        repo_root=repo_root,
    )
    if not active:
        return all_muted_ya, to_delete, False, added

    inherited = load_inherited_muted_lines(inherited_muted_ya_path)
    if not inherited:
        logging.info(
            'branch bootstrap grace active for %s (added %s) but inherited mute file is empty',
            branch,
            added,
        )
        return all_muted_ya, to_delete, True, added

    skipped_delete = sorted(set(to_delete) & inherited)
    merged_muted = sorted(set(all_muted_ya) | inherited)
    filtered_delete = sorted(set(to_delete) - inherited)

    logging.info(
        'branch bootstrap grace for %s: added=%s, grace_days=%s, inherited=%d, '
        'preserved_muted=+%d, skipped_to_delete=%d',
        branch,
        added,
        get_branch_bootstrap_grace_days(),
        len(inherited),
        len(merged_muted) - len(set(all_muted_ya)),
        len(skipped_delete),
    )
    return merged_muted, filtered_delete, True, added
