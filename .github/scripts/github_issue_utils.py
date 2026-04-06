#!/usr/bin/env python3

"""
Shared utilities for working with GitHub issues and parsing test names from issue bodies.
Used by both the muted test analytics and issue management scripts.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional


DEFAULT_BUILD_TYPE = 'relwithdebinfo'


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
        issues_data (list): List of issue dictionaries with 'body', 'url', 'title', 'issue_number' fields

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

            for test in parsed.tests:
                if test not in test_to_issue:
                    test_to_issue[test] = []
                test_to_issue[test].append({
                    'url': url,
                    'title': issue.get('title', ''),
                    'issue_number': issue.get('issue_number', 0),
                    'state': issue.get('state', ''),
                    'created_at': issue.get('created_at', 0),
                    'branches': parsed.branches,
                    'build_type': parsed.build_type,
                })
        except Exception as e:
            print(f"Warning: Could not parse issue body for issue {url}: {e}")
            continue

    return test_to_issue
