#!/usr/bin/env python3

"""
Shared utilities for working with GitHub issues and parsing test names from issue bodies.
Used by both the muted test analytics and issue management scripts.
"""

import datetime as dt
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

_GITHUB_TEAM_PREFIX = "TEAM:@ydb-platform/"


def team_slug_from_monitor_owner(owner) -> str:
    """Lowercase team slug from ``tests_monitor.owner`` / YQL ``owner`` (SQL-aligned).

    Strips ``TEAM:@ydb-platform/`` then lowercases the remainder.
    ``None`` → empty string (join keys / pandas use empty, not ``"unknown"``).
    """
    if owner is None:
        return ""
    s = str(owner).replace(_GITHUB_TEAM_PREFIX, "").strip()
    return s.lower()


DEFAULT_BUILD_TYPE = 'relwithdebinfo'
DEFAULT_BRANCH = 'main'

# Same strings as ``mute/update_mute_issues`` / ``mute.fast_unmute_pipeline`` (GraphQL labels).
MANUAL_FAST_UNMUTE_GITHUB_LABEL = 'manual-fast-unmute'
MANUAL_FAST_UNMUTE_FINISHED_GITHUB_LABEL = 'fast-unmute-finished'


def scan_to_utc_date(val) -> Optional[dt.date]:
    """YDB scan value → UTC calendar date.

    Handles both native_date_in_result_sets=True (returns dt.date / dt.datetime)
    and native_date_in_result_sets=False (returns int):
      - Date      → uint32 days since 1970-01-01         (< 100_000)
      - Datetime  → uint32 seconds since 1970-01-01      (< 10_000_000_000)
      - Timestamp → uint64 microseconds since 1970-01-01 (larger)
    """
    if val is None:
        return None
    if isinstance(val, dt.datetime):
        if val.tzinfo is not None:
            return val.astimezone(dt.timezone.utc).date()
        return val.date()
    if isinstance(val, dt.date):
        return val
    if isinstance(val, int):
        if val < 100_000:
            # YDB Date: days since Unix epoch
            return dt.date(1970, 1, 1) + dt.timedelta(days=val)
        if val < 10_000_000_000:
            # YDB Datetime: seconds since Unix epoch
            return dt.datetime.fromtimestamp(val, tz=dt.timezone.utc).date()
        # YDB Timestamp: microseconds since Unix epoch
        return dt.datetime.fromtimestamp(val / 1_000_000, tz=dt.timezone.utc).date()
    return None


def normalize_analytics_area(raw) -> str:
    """Match YQL ``$normalize``: first two ``/`` segments, else full string; empty → ``area/-``."""
    if raw is None:
        return "area/-"
    s = str(raw).strip()
    if not s:
        return "area/-"
    parts = s.split("/")
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    return s


def canonical_team_slug(raw_owner_team) -> str:
    """Lowercase team slug for routing (digest queue, Telegram ``teams``, GitHub project owner).

    Uses :func:`team_slug_from_monitor_owner` for the core strip/lowercase. Maps ``None``,
    empty, ``unknown`` (any case), and a bare ``TEAM:@ydb-platform/`` to the slug ``unknown``.
    """
    if raw_owner_team is None:
        return "unknown"
    raw = str(raw_owner_team).strip()
    if not raw or raw.lower() == "unknown":
        return "unknown"
    return team_slug_from_monitor_owner(raw) or "unknown"


# Backward-compatible alias used in older SQL comments and callers.
monitor_owner_to_team_key = team_slug_from_monitor_owner


def resolve_team_by_longest_area_prefix(normalized_area: str, area_to_owner: Dict[str, str]) -> Optional[str]:
    """Longest mapping key where area equals key or starts with key + '/'."""
    best = None
    best_len = -1
    for m_area, team in area_to_owner.items():
        if not m_area:
            continue
        if normalized_area == m_area or normalized_area.startswith(m_area + "/"):
            if len(m_area) > best_len:
                best = team
                best_len = len(m_area)
    return best


def area_to_owner_map_from_rows(rows: List[dict]) -> Dict[str, str]:
    """Normalized area → owner_team (last wins if duplicates)."""
    out: Dict[str, str] = {}
    for r in rows:
        a, ot = r.get("area"), r.get("owner_team")
        if not a or not ot:
            continue
        out[normalize_analytics_area(str(a))] = str(ot).strip()
    return out


def min_area_by_owner_team_from_rows(rows: List[dict]) -> Dict[str, str]:
    """Lowercase owner_team → ``MIN(normalize(area))`` lexicographic (same as SQL mart fallback)."""
    by_ot: Dict[str, List[str]] = defaultdict(list)
    for r in rows:
        a, ot = r.get("area"), r.get("owner_team")
        if not a or not ot:
            continue
        by_ot[str(ot).strip().lower()].append(normalize_analytics_area(str(a)))
    return {k: min(v) for k, v in by_ot.items() if v}


def pick_effective_analytics_area(
    area_override,
    area_override_since,
    date_window: dt.date,
    owner_team_key: str,
    min_area_by_owner: Dict[str, str],
) -> str:
    if area_override is not None and str(area_override).strip():
        na = normalize_analytics_area(area_override)
        if na:
            since = scan_to_utc_date(area_override_since)
            if since is None or date_window >= since:
                return na
    return min_area_by_owner.get(owner_team_key) or "area/-"


def effective_owner_team_for_area(
    effective_area: str, area_to_owner: Dict[str, str], owner_team_key: str
) -> str:
    mapped = resolve_team_by_longest_area_prefix(effective_area, area_to_owner)
    return str(mapped).strip().lower() if mapped else owner_team_key


def compute_effective_analytics_row(
    row: dict,
    gim_by_key: Dict[Tuple[str, str, str], dict],
    area_to_owner: Dict[str, str],
    min_area_by_owner: Dict[str, str],
) -> Tuple[str, str]:
    otk = team_slug_from_monitor_owner(row.get("owner"))
    key = (str(row["full_name"]), str(row["branch"]), str(row["build_type"]))
    g = gim_by_key.get(key, {})
    dw = row["date_window"]
    if isinstance(dw, dt.datetime):
        dw = dw.date()
    eff_area = pick_effective_analytics_area(
        g.get("area_override"),
        g.get("area_override_since"),
        dw,
        otk,
        min_area_by_owner,
    )
    eff_ot = effective_owner_team_for_area(eff_area, area_to_owner, otk)
    return eff_area, eff_ot


@dataclass
class ParsedIssueBody:
    tests: List[str] = field(default_factory=list)
    branches: List[str] = field(default_factory=lambda: ['main'])
    build_type: str = DEFAULT_BUILD_TYPE


def _extract_between_markers(body: str, start_marker: str, end_marker: str) -> Optional[str]:
    """Return text between two HTML comment markers, or None if markers are absent."""
    if start_marker not in body or end_marker not in body:
        return None
    idx1 = body.find(start_marker)
    idx2 = body.find(end_marker)
    return body[idx1 + len(start_marker) + 1 : idx2]


def parse_body(body: str) -> ParsedIssueBody:
    """Parse GitHub issue body to extract test names, branches and build_type.

    Args:
        body: The GitHub issue body text

    Returns:
        ParsedIssueBody with extracted fields (all have sensible defaults).
    """
    result = ParsedIssueBody()

    # --- tests ---
    mute_block = _extract_between_markers(body, "<!--mute_list_start-->", "<!--mute_list_end-->")
    if mute_block is not None:
        lines = mute_block.split('\n')
    else:
        prepared_body = ''
        if body.startswith('Mute:'):
            prepared_body = body.split('Mute:', 1)[1].strip()
        elif body.startswith('Mute'):
            prepared_body = body.split('Mute', 1)[1].strip()
        elif body.startswith('ydb'):
            prepared_body = body
        lines = prepared_body.split('**Add line to')[0].split('\n')
    result.tests = [line.strip() for line in lines if line.strip().startswith('ydb/')]

    # --- branches ---
    branch_block = _extract_between_markers(body, "<!--branch_list_start-->", "<!--branch_list_end-->")
    if branch_block is not None:
        result.branches = [b.strip() for b in branch_block.split('\n') if b.strip()]

    # --- build_type ---
    bt_block = _extract_between_markers(body, "<!--build_type_list_start-->", "<!--build_type_list_end-->")
    if bt_block is not None:
        val = bt_block.strip()
        if val:
            result.build_type = val

    return result


def _decode_issues_json_column(val):
    """YDB ``Json`` / UTF8 JSON / scan row → Python ``dict`` / ``list`` or ``None``."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return None
    return None


def issue_label_names_lower(raw_labels) -> frozenset:
    """Parse ``issues.labels`` JSON (export schema) → frozenset of lowercase label names."""
    decoded = _decode_issues_json_column(raw_labels)
    if not decoded:
        return frozenset()
    names = []
    for item in decoded:
        if isinstance(item, dict):
            n = item.get('name')
            if n:
                names.append(str(n).strip().lower())
    return frozenset(names)


def build_github_issue_mapping_info_snapshot(issue_row: dict) -> dict:
    """JSON payload for ``github_issue_mapping.info`` (assignees, labels, project fields, …)."""
    assignees = _decode_issues_json_column(issue_row.get('assignees')) or []
    labels = _decode_issues_json_column(issue_row.get('labels')) or []
    milestone = _decode_issues_json_column(issue_row.get('milestone'))
    project_fields = _decode_issues_json_column(issue_row.get('project_fields'))
    raw_info = issue_row.get('info')
    if isinstance(raw_info, str):
        try:
            info_extra = json.loads(raw_info) if raw_info.strip() else {}
        except json.JSONDecodeError:
            info_extra = {}
    elif isinstance(raw_info, dict):
        info_extra = raw_info
    else:
        info_extra = {}

    out = {
        'assignees': assignees,
        'labels': labels,
        'issue_type': issue_row.get('issue_type'),
        'author_login': issue_row.get('author_login'),
        'repository_name': issue_row.get('repository_name'),
        'project_status': issue_row.get('project_status'),
        'project_owner': issue_row.get('project_owner'),
        'project_priority': issue_row.get('project_priority'),
        'info': info_extra,
    }
    if milestone is not None:
        out['milestone'] = milestone
    if project_fields is not None:
        out['project_fields'] = project_fields
    return out


def make_profile_id(branch: str, build_type: str) -> str:
    """Canonical profile_id used by digest_queue and notification config.

    Format is ``branch:build_type`` (colon) so build presets like ``release-asan``
    stay unambiguous. Legacy rows may still use ``branch-build_type``; migrate
    those in YDB if you need them picked up by the new code.
    """
    return f"{branch}:{build_type}"


def create_test_issue_mapping(issues_data):
    """Create a mapping from test names to GitHub issue information

    Args:
        issues_data (list): Rows from YDB ``issues`` (or compatible dicts with body, url, labels, …).

    Returns:
        dict: Mapping from test name to list of issue information
    """
    test_to_issue = {}

    for issue in issues_data:
        body = issue.get('body', '')
        url = issue.get('url', '')

        if not body or not url:
            continue

        try:
            parsed = parse_body(body)
            snapshot = build_github_issue_mapping_info_snapshot(issue)

            for test in parsed.tests:
                if test not in test_to_issue:
                    test_to_issue[test] = []
                test_to_issue[test].append({
                    'url': url,
                    'title': issue.get('title', ''),
                    'issue_number': issue.get('issue_number', 0),
                    'state': issue.get('state', ''),
                    'state_reason': issue.get('state_reason'),
                    'created_at': issue.get('created_at', 0),
                    'branches': parsed.branches,
                    'build_type': parsed.build_type,
                    'mapping_info': snapshot,
                })
        except Exception as e:
            print(f"Warning: Could not parse issue body for issue {url}: {e}")
            continue

    return test_to_issue
