#!/usr/bin/env python3

"""
Create a mapping table between test names and GitHub issues.
This table will be used by SQL queries to join muted test data with GitHub issue information.

owner_override logic
--------------------
Each muted-test issue has a default owner (from TESTOWNERS via the ``Owner:`` line in the
issue body).  A human can reassign responsibility by adding an ``area/xxx`` label to the
issue.  If the team behind that area (looked up via ``area_to_owner_mapping``) differs from
the default owner, we store it as ``owner_override``.

Edge cases:
  * No ``area/`` label → owner_override = NULL
  * ``area/`` resolves to the same team as the default owner → owner_override = NULL
  * ``area/`` label not found in mapping → owner_override = NULL
  * Label changed multiple times → we always see the *current* set of labels
"""

import json
import os
import re
import ydb
import time
import sys
from ydb_wrapper import YDBWrapper

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from github_issue_utils import create_test_issue_mapping, DEFAULT_BUILD_TYPE


def get_github_issues_data(ydb_wrapper):
    """Get GitHub issues data from the issues table, including labels info."""
    issues_table = ydb_wrapper.get_table_path("issues")
    query = f"""
    SELECT
        issue_number,
        title,
        url,
        state,
        body,
        created_at,
        updated_at,
        info
    FROM `{issues_table}`
    WHERE body IS NOT NULL
    AND body != ''
    """

    print("Fetching GitHub issues data...")

    try:
        results = ydb_wrapper.execute_scan_query(query)
        print(f'Fetched {len(results)} GitHub issues')
        return results
    except Exception as e:
        print(f"Warning: Could not fetch GitHub issues data: {e}")
        print("This might be because the github_data/issues table doesn't exist yet.")
        return []


def get_area_to_owner_mapping(ydb_wrapper):
    """Load area -> owner_team mapping from YDB."""
    try:
        table_path = ydb_wrapper.get_table_path("area_to_owner_mapping")
        rows = ydb_wrapper.execute_scan_query(
            f"SELECT area, owner_team FROM `{table_path}`",
            query_name="get_area_to_owner_mapping",
        )
        mapping = {}
        for row in rows:
            area = row.get("area", "")
            owner = row.get("owner_team", "")
            if area and owner:
                mapping[area] = owner
        print(f"Loaded area_to_owner_mapping: {len(mapping)} entries")
        return mapping
    except Exception as e:
        print(f"Warning: Could not load area_to_owner_mapping: {e}")
        return {}


_OWNER_RE = re.compile(r'Owner:\s*(?:TEAM:@ydb-platform/)?(\S+)', re.IGNORECASE)


def _extract_default_owner(body: str) -> str:
    """Extract the default owner team name from issue body ``Owner:`` line.

    Returns lowercase team name or empty string.
    """
    if not body:
        return ""
    m = _OWNER_RE.search(body)
    return m.group(1).lower() if m else ""


def _extract_area_from_info(info_raw) -> str:
    """Extract area/ label value from the ``info`` JSON column.

    Returns the area string (e.g. 'area/queryprocessor') or empty string.
    """
    if not info_raw:
        return ""
    try:
        if isinstance(info_raw, str):
            info = json.loads(info_raw)
        elif isinstance(info_raw, dict):
            info = info_raw
        else:
            info = json.loads(str(info_raw))
    except (json.JSONDecodeError, TypeError):
        return ""
    return info.get("area") or ""


def resolve_owner_override(body: str, info_raw, area_to_owner: dict) -> str:
    """Determine owner_override for an issue.

    Returns the overriding team name (lowercase) or None.
    """
    area_label = _extract_area_from_info(info_raw)
    if not area_label:
        return None

    resolved_team = area_to_owner.get(area_label, "")
    if not resolved_team:
        return None

    default_owner = _extract_default_owner(body)

    if resolved_team.lower() == default_owner.lower():
        return None

    return resolved_team.lower()


def create_test_issue_mapping_table(ydb_wrapper, table_path):
    """Create the test-to-issue mapping table"""
    print(f"Creating test-to-issue mapping table: {table_path}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name` Utf8 NOT NULL,
        `branch` Utf8 NOT NULL,
        `build_type` Utf8 NOT NULL,
        `github_issue_url` Utf8,
        `github_issue_title` Utf8,
        `github_issue_number` Uint64 NOT NULL,
        `github_issue_state` Utf8 NOT NULL,
        `github_issue_created_at` Timestamp,
        `owner_override` Utf8,
        PRIMARY KEY (full_name, branch, build_type, github_issue_number, github_issue_state)
    )
    PARTITION BY HASH(full_name)
    WITH (
        STORE = COLUMN
    )
    """

    print(f"Creating table with query: {create_table_sql}")
    ydb_wrapper.create_table(table_path, create_table_sql)


def convert_mapping_to_table_data(test_to_issue_mapping):
    """Convert the test-to-issue mapping to table data format"""
    table_data = []

    for test_name, issues in test_to_issue_mapping.items():
        if issues:
            sorted_issues = sorted(issues, key=lambda x: x.get('created_at', 0), reverse=True)
            latest_issue = sorted_issues[0]

            for branch in latest_issue['branches']:
                table_data.append({
                    'full_name': test_name,
                    'branch': branch,
                    'build_type': latest_issue.get('build_type', DEFAULT_BUILD_TYPE),
                    'github_issue_url': latest_issue['url'],
                    'github_issue_title': latest_issue['title'],
                    'github_issue_number': latest_issue['issue_number'],
                    'github_issue_state': latest_issue['state'],
                    'github_issue_created_at': latest_issue.get('created_at'),
                    'owner_override': latest_issue.get('owner_override'),
                })

    return table_data


def bulk_upsert_mapping_data(ydb_wrapper, table_path, mapping_data):
    """Bulk upsert mapping data into the table"""
    print(f"Bulk upserting {len(mapping_data)} test-to-issue mappings to {table_path}")

    column_types = ydb.BulkUpsertColumns()
    column_types.add_column('full_name', ydb.PrimitiveType.Utf8)
    column_types.add_column('branch', ydb.PrimitiveType.Utf8)
    column_types.add_column('build_type', ydb.PrimitiveType.Utf8)
    column_types.add_column('github_issue_url', ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column('github_issue_title', ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column('github_issue_number', ydb.OptionalType(ydb.PrimitiveType.Uint64))
    column_types.add_column('github_issue_state', ydb.PrimitiveType.Utf8)
    column_types.add_column('github_issue_created_at', ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    column_types.add_column('owner_override', ydb.OptionalType(ydb.PrimitiveType.Utf8))

    ydb_wrapper.bulk_upsert(table_path, mapping_data, column_types)
    print(f"Bulk upsert completed")


def main():
    """Main function to create the test-to-issue mapping table"""
    print("Starting GitHub issue mapping table creation")
    script_start_time = time.time()

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        table_path = ydb_wrapper.get_table_path("github_issue_mapping")

        try:
            issues_data = get_github_issues_data(ydb_wrapper)

            if not issues_data:
                print("No GitHub issues data found")
                return 0

            area_to_owner = get_area_to_owner_mapping(ydb_wrapper)

            # Pre-compute owner_override per issue URL for O(1) lookup
            url_to_override = {}
            for issue in issues_data:
                url = issue.get('url', '')
                if url:
                    url_to_override[url] = resolve_owner_override(
                        issue.get('body', ''),
                        issue.get('info'),
                        area_to_owner,
                    )

            print("Creating test-to-issue mapping...")
            test_to_issue = create_test_issue_mapping(issues_data)
            print(f"Created mapping for {len(test_to_issue)} unique test names")

            override_count = 0
            for issue_list in test_to_issue.values():
                for issue_info in issue_list:
                    override = url_to_override.get(issue_info['url'])
                    issue_info['owner_override'] = override
                    if override:
                        override_count += 1

            if override_count:
                print(f"Resolved {override_count} owner_override(s) from area/ labels")

            mapping_data = convert_mapping_to_table_data(test_to_issue)
            print(f"Converted to {len(mapping_data)} table records")

            create_test_issue_mapping_table(ydb_wrapper, table_path)

            if mapping_data:
                bulk_upsert_mapping_data(ydb_wrapper, table_path, mapping_data)
            else:
                print("No mapping data to insert")

            script_elapsed = time.time() - script_start_time
            print(f"Script completed successfully, total time: {script_elapsed:.2f}s")

        except Exception as e:
            print(f"Error during execution: {e}")
            return 1

        return 0


if __name__ == "__main__":
    exit(main())
