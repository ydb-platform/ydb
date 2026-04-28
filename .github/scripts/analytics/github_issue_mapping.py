#!/usr/bin/env python3

"""
Create a mapping table between test names and GitHub issues.
This table will be used by SQL queries to join muted test data with GitHub issue information.

area_override logic
-------------------
Each muted-test issue has a default owner (from TESTOWNERS via the ``Owner:`` line in the
issue body).  A human can add an ``area/...`` label (e.g. ``area/blobstorage``).  If the
team behind that area (via ``area_to_owner_mapping``) **differs** from the default owner,
we store the **area path string** in ``area_override``. ``tests_monitor.py`` reads this table
(plus ``area_to_owner_mapping``) and fills ``effective_area`` / ``effective_owner_team`` on each
row; downstream marts use those columns.

Edge cases:
  * No ``area`` in issue ``info`` → area_override = NULL
  * Closed **NOT_PLANNED** / **DUPLICATE** → area_override = NULL (issues excluded from mapping query)
  * Closed **COMPLETED** with ``manual-fast-unmute`` label → override can apply (fast-track window)
  * Closed **COMPLETED** without that label: override cleared **only** when **project Status** is
    ``Unmuted`` (do not infer from closer — avoids clearing during the gap before fast-unmute label).
  * Area resolves to the same team as the default owner → area_override = NULL
  * Area not found in mapping → area_override = NULL
  * Labels change → we always see the *current* ``info`` snapshot

``github_issue_state_reason`` / ``info`` (Json): GitHub close reason and a snapshot (assignees,
labels, project fields, …). For existing deployments run ``ALTER TABLE``; ``CREATE TABLE`` below
is for new installs only.

``area_override_since`` (Date)
-------------------------------
First ``date_window`` for which datamarts apply ``area_override``. Set from the issue's
``updated_at`` (UTC date) when the override **value** changes vs the previous row in YDB;
unchanged override keeps the stored date. ``NULL`` on ``area_override_since`` means there is
no lower bound: override applies for every ``date_window`` in the mart query (same as omitting
the check in SQL).
"""

import datetime as dt
import json
import os
import re
import ydb
import time
import sys
from ydb_wrapper import YDBWrapper

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from tests.mute.update_mute_issues import MANUAL_FAST_UNMUTE_GITHUB_LABEL
from github_issue_utils import (
    create_test_issue_mapping,
    DEFAULT_BUILD_TYPE,
    issue_label_names_lower,
    scan_to_utc_date,
)


def get_github_issues_data(ydb_wrapper):
    """Get GitHub issues data from the issues table, including labels info.

    Excludes issues closed as **Not planned** or **Duplicate** so they cannot win
    ``latest per build_type`` in :func:`convert_mapping_to_table_data`.
    """
    issues_table = ydb_wrapper.get_table_path("issues")
    query = f"""
    SELECT
        issue_number,
        title,
        url,
        state,
        state_reason,
        body,
        created_at,
        updated_at,
        info,
        assignees,
        labels,
        milestone,
        project_fields,
        issue_type,
        author_login,
        repository_name,
        project_status,
        project_owner,
        project_priority
    FROM `{issues_table}`
    WHERE body IS NOT NULL
    AND body != ''
    AND NOT (
        state = 'CLOSED'
        AND state_reason IN ('NOT_PLANNED', 'DUPLICATE')
    )
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


def _norm_area_override_value(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _fetch_existing_github_issue_mapping(ydb_wrapper, table_path: str) -> dict | None:
    """Return dict keyed by (full_name, branch, build_type, github_issue_number), or None on failure."""
    try:
        rows = ydb_wrapper.execute_scan_query(
            f"""
            SELECT
                full_name,
                branch,
                build_type,
                github_issue_number,
                area_override,
                area_override_since
            FROM `{table_path}`
            """,
            query_name="github_issue_mapping_read_existing_for_since",
        )
    except Exception as e:
        print(
            f"Error: cannot read `{table_path}` (add column area_override_since or migrate): {e}",
            file=sys.stderr,
        )
        return None
    out = {}
    for r in rows:
        key = (r["full_name"], r["branch"], r["build_type"], r["github_issue_number"])
        out[key] = r
    return out


def merge_area_override_since(mapping_data: list, existing_by_key: dict, url_to_updated_at: dict) -> None:
    """Set area_override_since on each row in place (mutates mapping_data)."""
    today_utc = dt.datetime.now(dt.timezone.utc).date()
    for row in mapping_data:
        key = (
            row["full_name"],
            row["branch"],
            row["build_type"],
            row["github_issue_number"],
        )
        old = existing_by_key.get(key)
        new_ao = _norm_area_override_value(row.get("area_override"))
        old_ao = _norm_area_override_value(old.get("area_override")) if old else None
        url = row.get("github_issue_url") or ""
        since_from_issue = scan_to_utc_date(url_to_updated_at.get(url))
        if new_ao is None:
            row["area_override_since"] = None
        elif old is None or old_ao != new_ao:
            row["area_override_since"] = since_from_issue or today_utc
        else:
            row["area_override_since"] = scan_to_utc_date(old.get("area_override_since"))


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


def _resolve_area_to_team(area_label: str, area_to_owner: dict) -> str:
    """Prefix match: area/cs/compression -> area/cs (longest mapping key wins).

    Mirrors the SQL logic: ``WHERE a.area = om.area OR StartsWith(a.area, om.area || '/')``
    with ``ORDER BY LENGTH(om.area) DESC``.
    """
    best_team = ""
    best_len = -1
    for mapping_area, team in area_to_owner.items():
        if area_label == mapping_area or area_label.startswith(mapping_area + '/'):
            if len(mapping_area) > best_len:
                best_team = team
                best_len = len(mapping_area)
    return best_team


def resolve_area_override(
    body: str,
    info_raw,
    area_to_owner: dict,
    issue_state=None,
    state_reason=None,
    labels_raw=None,
    project_status=None,
):
    """If GitHub ``area/...`` implies a different team than ``Owner:``, return that area path.

    **Closed issues**

    * ``NOT_PLANNED`` / ``DUPLICATE``: no override.
    * ``COMPLETED`` (or empty ``state_reason`` on **CLOSED**) + label ``manual-fast-unmute``: override stays (fast-track active).
    * ``COMPLETED`` (or empty ``state_reason``) without that label: override cleared **only** when **project Status** is ``Unmuted``.

    Open issues: unchanged area logic.

    Stored value matches issue info (e.g. ``area/blobstorage``), not the resolved team slug.
    """
    st = (issue_state or "").strip().upper()
    sr = (state_reason or "").strip().upper()
    labels_lo = issue_label_names_lower(labels_raw)

    if st == "CLOSED":
        if sr in ("NOT_PLANNED", "DUPLICATE"):
            return None
        # Empty ``state_reason`` in export/API: treat like COMPLETED for override / Unmuted logic.
        if sr == "COMPLETED" or not sr:
            if MANUAL_FAST_UNMUTE_GITHUB_LABEL.lower() in labels_lo:
                pass
            else:
                ps = (project_status or "").strip().lower()
                if ps == "unmuted":
                    return None
        else:
            return None

    area_label = (_extract_area_from_info(info_raw) or "").strip()
    if not area_label:
        return None

    resolved_team = _resolve_area_to_team(area_label, area_to_owner)
    if not resolved_team:
        return None

    default_owner = _extract_default_owner(body)

    if resolved_team.lower() == default_owner.lower():
        return None

    return area_label


def create_test_issue_mapping_table(ydb_wrapper, table_path):
    """Create the test-to-issue mapping table"""
    print(f"Creating test-to-issue mapping table: {table_path}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name`               Utf8       NOT NULL,
        `branch`                  Utf8       NOT NULL,
        `build_type`              Utf8       NOT NULL,
        `github_issue_url`        Utf8,
        `github_issue_title`      Utf8,
        `github_issue_number`     Uint64     NOT NULL,
        `github_issue_state`        Utf8,
        `github_issue_state_reason` Utf8,
        `github_issue_created_at`   Timestamp,
        `area_override`             Utf8,
        `area_override_since`       Date,
        `info`                      Json,
        PRIMARY KEY (full_name, branch, build_type, github_issue_number)
    )
    PARTITION BY HASH(full_name)
    WITH (STORE = COLUMN)
    """

    print(f"Creating table with query: {create_table_sql}")
    ydb_wrapper.create_table(table_path, create_table_sql)


def convert_mapping_to_table_data(test_to_issue_mapping):
    """Convert the test-to-issue mapping to table data format"""
    table_data = []

    for test_name, issues in test_to_issue_mapping.items():
        if not issues:
            continue

        # Group issues by build_type, then pick the latest created issue per group.
        # Issues closed as not-planned/duplicate are omitted upstream (get_github_issues_data).
        by_build_type = {}
        for issue in issues:
            bt = issue.get('build_type', DEFAULT_BUILD_TYPE)
            existing = by_build_type.get(bt)
            if existing is None or issue.get('created_at', 0) > existing.get('created_at', 0):
                by_build_type[bt] = issue

        for bt, latest_issue in by_build_type.items():
            snap = dict(latest_issue.get('mapping_info') or {})
            ao = latest_issue.get('area_override')
            if ao:
                snap['area_override'] = ao
            info_json = json.dumps(snap, ensure_ascii=False)
            for branch in latest_issue['branches']:
                table_data.append({
                    'full_name': test_name,
                    'branch': branch,
                    'build_type': bt,
                    'github_issue_url': latest_issue['url'],
                    'github_issue_title': latest_issue['title'],
                    'github_issue_number': latest_issue['issue_number'],
                    'github_issue_state': latest_issue['state'],
                    'github_issue_state_reason': latest_issue.get('state_reason'),
                    'github_issue_created_at': latest_issue.get('created_at'),
                    'area_override': latest_issue.get('area_override'),
                    'info': info_json,
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
    column_types.add_column('github_issue_number', ydb.PrimitiveType.Uint64)
    column_types.add_column('github_issue_state', ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column(
        'github_issue_state_reason', ydb.OptionalType(ydb.PrimitiveType.Utf8)
    )
    column_types.add_column('github_issue_created_at', ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    column_types.add_column('area_override', ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column('area_override_since', ydb.OptionalType(ydb.PrimitiveType.Date))
    column_types.add_column('info', ydb.OptionalType(ydb.PrimitiveType.Json))

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

            url_to_updated_at = {}
            # Pre-compute area_override per issue URL for O(1) lookup
            url_to_area_override = {}
            for issue in issues_data:
                url = issue.get('url', '')
                if url:
                    url_to_updated_at[url] = issue.get("updated_at") or issue.get("created_at")
                    url_to_area_override[url] = resolve_area_override(
                        issue.get('body', ''),
                        issue.get('info'),
                        area_to_owner,
                        issue.get('state'),
                        issue.get('state_reason'),
                        issue.get('labels'),
                        issue.get('project_status'),
                    )

            print("Creating test-to-issue mapping...")
            test_to_issue = create_test_issue_mapping(issues_data)
            print(f"Created mapping for {len(test_to_issue)} unique test names")

            override_count = 0
            for issue_list in test_to_issue.values():
                for issue_info in issue_list:
                    ao = url_to_area_override.get(issue_info['url'])
                    issue_info['area_override'] = ao
                    if ao:
                        override_count += 1

            if override_count:
                print(f"Resolved {override_count} area_override(s) from area labels")

            mapping_data = convert_mapping_to_table_data(test_to_issue)
            print(f"Converted to {len(mapping_data)} table records")

            create_test_issue_mapping_table(ydb_wrapper, table_path)

            existing_by_key = _fetch_existing_github_issue_mapping(ydb_wrapper, table_path)
            if existing_by_key is None:
                return 1
            merge_area_override_since(mapping_data, existing_by_key, url_to_updated_at)

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
