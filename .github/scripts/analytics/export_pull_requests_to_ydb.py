#!/usr/bin/env python3

"""Export GitHub pull requests to YDB.

Modes:
  default          PRs updated in last 24 hours
  --days N         PRs updated in last N days (e.g. python export_pull_requests_to_ydb.py --days 60)
  --full           All PRs
"""

import os
import ydb
import time
import json
import argparse
from datetime import datetime, timezone, timedelta
import requests
from typing import List, Dict, Any, Optional
from ydb_wrapper import YDBWrapper

# Configuration
ORG_NAME = 'ydb-platform'
REPO_NAME = 'ydb'

# Column schema: (name, ydb_type, sql_type, nullable)
# Single source of truth for CREATE TABLE and BulkUpsertColumns
COLUMNS_SCHEMA = [
    ("project_item_id", ydb.PrimitiveType.Utf8, "Utf8", False),
    ("pr_id", ydb.PrimitiveType.Utf8, "Utf8", False),
    ("pr_number", ydb.PrimitiveType.Uint64, "Uint64", False),
    ("title", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("url", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("state", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("body", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("body_text", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("is_draft", ydb.PrimitiveType.Int32, "Int", False),
    ("additions", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("deletions", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("changed_files", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("mergeable", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("merged", ydb.PrimitiveType.Int32, "Int", False),
    ("created_at", ydb.PrimitiveType.Timestamp, "Timestamp", False),
    ("updated_at", ydb.PrimitiveType.Timestamp, "Timestamp", True),
    ("closed_at", ydb.PrimitiveType.Timestamp, "Timestamp", True),
    ("merged_at", ydb.PrimitiveType.Timestamp, "Timestamp", True),
    ("created_date", ydb.PrimitiveType.Date, "Date", False),
    ("updated_date", ydb.PrimitiveType.Date, "Date", False),
    ("days_since_created", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("days_since_updated", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("time_to_close_hours", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("time_to_merge_hours", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("author_login", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("author_url", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("repository_name", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("repository_url", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("head_ref_name", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("base_ref_name", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("assignees", ydb.PrimitiveType.Json, "Json", True),
    ("labels", ydb.PrimitiveType.Json, "Json", True),
    ("milestone", ydb.PrimitiveType.Json, "Json", True),
    ("project_fields", ydb.PrimitiveType.Json, "Json", True),
    ("review_decision", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("total_reviews_count", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("total_comments_count", ydb.PrimitiveType.Uint64, "Uint64", True),
    ("merged_by_login", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("merged_by_url", ydb.PrimitiveType.Utf8, "Utf8", True),
    ("info", ydb.PrimitiveType.Json, "Json", True),
    ("exported_at", ydb.PrimitiveType.Timestamp, "Timestamp", False),
]


def _build_column_types() -> ydb.BulkUpsertColumns:
    """Build BulkUpsertColumns from COLUMNS_SCHEMA."""
    columns = ydb.BulkUpsertColumns()
    for name, ydb_type, _, _ in COLUMNS_SCHEMA:
        columns.add_column(name, ydb.OptionalType(ydb_type))
    return columns


def _build_create_table_sql(table_path: str) -> str:
    """Build CREATE TABLE SQL from COLUMNS_SCHEMA."""
    col_defs = []
    for name, _, sql_type, nullable in COLUMNS_SCHEMA:
        null_str = "" if nullable else " NOT NULL"
        col_defs.append(f"            `{name}` {sql_type}{null_str}")
    columns_sql = ",\n".join(col_defs)
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
{columns_sql},
            PRIMARY KEY (`created_date`, `pr_number`, `project_item_id`)
        )
        PARTITION BY HASH(`created_date`)
        WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_BY_SIZE = ENABLED,
            AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4
        )
    """


def run_query(query: str, variables: Optional[Dict] = None) -> Dict[str, Any]:
    """Execute GraphQL query against GitHub API. Retries on 502/503/504/429."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is required")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    max_attempts = 4
    retry_statuses = (502, 503, 504, 429)
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            request = requests.post(
                'https://api.github.com/graphql',
                json={'query': query, 'variables': variables},
                headers=headers,
                timeout=60,
            )
            if request.status_code in retry_statuses:
                last_error = f"Query failed with status {request.status_code}: {request.text[:200]}"
                if attempt < max_attempts:
                    delay = 5 * attempt
                    print(f"GitHub API returned {request.status_code}, retrying in {delay}s (attempt {attempt}/{max_attempts})...")
                    time.sleep(delay)
                    continue
                raise Exception(last_error)
            if request.status_code != 200:
                raise Exception(f"Query failed with status {request.status_code}: {request.text}")
            response = request.json()
            if 'errors' in response:
                for err in response['errors']:
                    msg = err.get('message', 'Unknown error')
                    print(f"GraphQL Error: {msg}")
                raise Exception(f"GraphQL Error: {response['errors'][0].get('message', 'Unknown error')}")
            return response
        except requests.exceptions.Timeout as e:
            last_error = str(e)
            if attempt < max_attempts:
                delay = 5 * attempt
                print(f"Request timeout, retrying in {delay}s (attempt {attempt}/{max_attempts})...")
                time.sleep(delay)
                continue
            raise
    raise Exception(last_error or "run_query failed")


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse GitHub ISO datetime string to datetime (UTC)."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def fetch_repository_pull_requests(
    org_name: str,
    repo_name: str,
    updated_since: Optional[datetime] = None,
    updated_until: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Fetch pull requests from repository. If updated_since is set, stop when all remaining PRs are older.
    If updated_until is set, include only PRs with updated_at < updated_until."""
    if updated_since and updated_until:
        print(f"Fetching PRs updated in [{updated_since.date().isoformat()}, {updated_until.date().isoformat()}) from {org_name}/{repo_name}...")
    elif updated_since:
        print(f"Fetching PRs updated since {updated_since.isoformat()} from {org_name}/{repo_name}...")
    else:
        print(f"Fetching all PRs from {org_name}/{repo_name}...")
    start_time = time.time()
    prs: List[Dict[str, Any]] = []
    has_next_page = True
    end_cursor = "null"

    # first: 25 — larger pages often hit 502/504 (GitHub gateway timeout)
    query_template = """
    {
      organization(login: "%s") {
        repository(name: "%s") {
          pullRequests(first: 25, after: %s, orderBy: {field: UPDATED_AT, direction: DESC}) {
            nodes {
              id
              number
              title
              url
              state
              isDraft
              body
              bodyText
              createdAt
              updatedAt
              closedAt
              mergedAt
              additions
              deletions
              changedFiles
              mergeable
              merged
              author { login url }
              assignees(first: 10) { nodes { login url } }
              labels(first: 20) { nodes { id name color description } }
              milestone { id title url state dueOn }
              mergedBy { login url }
              reviewDecision
              reviews(first: 1) { totalCount }
              comments(first: 1) { totalCount }
              headRefName
              baseRefName
              repository { name url }
              headRepository { name url owner { login } }
              projectItems(first: 10) {
                nodes {
                  project { title number }
                  fieldValues(first: 20) {
                    nodes {
                      ... on ProjectV2ItemFieldSingleSelectValue {
                        field { ... on ProjectV2SingleSelectField { name } }
                        name
                      }
                      ... on ProjectV2ItemFieldTextValue {
                        field { ... on ProjectV2Field { name } }
                        text
                      }
                      ... on ProjectV2ItemFieldNumberValue {
                        field { ... on ProjectV2Field { name } }
                        number
                      }
                      ... on ProjectV2ItemFieldDateValue {
                        field { ... on ProjectV2Field { name } }
                        date
                      }
                      ... on ProjectV2ItemFieldIterationValue {
                        field { ... on ProjectV2IterationField { name } }
                        title
                      }
                    }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
    """

    while has_next_page:
        q = query_template % (org_name, repo_name, end_cursor)
        result = run_query(q)
        if not result or 'data' not in result:
            break
        repo = result['data']['organization']['repository']
        conn = repo['pullRequests']
        nodes = conn.get('nodes') or []
        page_info = conn.get('pageInfo') or {}

        for node in nodes:
            if updated_since is not None:
                updated_at = parse_datetime(node.get('updatedAt'))
                if updated_at is None or updated_at < updated_since:
                    # Rest of page and all next pages are older; stop
                    has_next_page = False
                    break
                if updated_until is not None and updated_at >= updated_until:
                    # Skip PRs newer than window (e.g. next month)
                    continue
            prs.append(node)
        else:
            # No break: full page was within range
            total = len(prs)
            # Вычисляем диапазон дат для собранных PR
            if prs:
                dates = [parse_datetime(pr.get('updatedAt')) for pr in prs if pr.get('updatedAt')]
                dates = [d for d in dates if d is not None]
                if dates:
                    min_date = min(dates).date().isoformat()
                    max_date = max(dates).date().isoformat()
                    print(f"Fetched {len(nodes)} PRs (total: {total}, date range: {min_date} to {max_date})")
                else:
                    print(f"Fetched {len(nodes)} PRs (total: {total})")
            else:
                print(f"Fetched {len(nodes)} PRs (total: {total})")
            has_next_page = page_info.get('hasNextPage', False)
            end_cursor = f'"{page_info["endCursor"]}"' if page_info.get('endCursor') else "null"
            if has_next_page:
                time.sleep(1.5)  # Throttle to reduce 502/504 from GitHub
            continue
        break

    elapsed = time.time() - start_time
    print(f"Fetched {len(prs)} PRs total (took {elapsed:.2f}s)")
    return prs


def _str(v: Any) -> str:
    """Return v if truthy, else empty string. Handles None from API."""
    return v if v else ""


def _build_info(pr: Dict[str, Any]) -> Optional[str]:
    """Build info JSON with head repository data (for fork PRs)."""
    head_repo = pr.get("headRepository")
    if not head_repo:
        return None
    
    info = {
        "head_repository": {
            "name": head_repo.get("name"),
            "url": head_repo.get("url"),
            "owner": (head_repo.get("owner") or {}).get("login"),
        }
    }
    return json.dumps(info)


def _extract_project_fields(pr_node: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Extract project fields from PR's projectItems.
    
    Returns dict: {project_title: {field_name: value, ...}, ...}
    """
    project_items = pr_node.get("projectItems", {}).get("nodes") or []
    result: Dict[str, Dict[str, Any]] = {}
    
    for item in project_items:
        project = item.get("project") or {}
        project_title = project.get("title")
        if not project_title:
            continue
        
        fields: Dict[str, Any] = {}
        for fv in (item.get("fieldValues") or {}).get("nodes") or []:
            field_info = fv.get("field") or {}
            field_name = field_info.get("name")
            if not field_name:
                continue
            
            # Extract value based on field type
            if "name" in fv:  # SingleSelectValue
                fields[field_name] = fv["name"]
            elif "text" in fv:  # TextValue
                fields[field_name] = fv["text"]
            elif "number" in fv:  # NumberValue
                fields[field_name] = fv["number"]
            elif "date" in fv:  # DateValue
                fields[field_name] = fv["date"]
            elif "title" in fv:  # IterationValue
                fields[field_name] = fv["title"]
        
        if fields:
            result[project_title] = fields
    
    return result


def transform_pull_requests_for_ydb(pr_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform GraphQL PR nodes into YDB row dicts."""
    print("Transforming PRs for YDB...")
    start_time = time.time()
    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for pr in pr_nodes:
        pr_number = pr.get('number')
        # Extract project fields from projectItems in the PR itself
        pr_project = _extract_project_fields(pr)
        author = pr.get('author') or {}
        merged_by = pr.get('mergedBy') or {}
        assignees = [
            {"login": a.get("login", ""), "url": a.get("url", "")}
            for a in (pr.get("assignees") or {}).get("nodes", [])
        ]
        labels = [
            {"id": l.get("id"), "name": l.get("name"), "color": l.get("color"), "description": l.get("description")}
            for l in (pr.get("labels") or {}).get("nodes", [])
        ]
        milestone = pr.get("milestone")
        milestone_info = None
        if milestone:
            milestone_info = {
                "id": milestone.get("id"),
                "title": milestone.get("title"),
                "url": milestone.get("url"),
                "state": milestone.get("state"),
                "dueOn": milestone.get("dueOn"),
            }
        created_at = parse_datetime(pr.get("createdAt"))
        updated_at = parse_datetime(pr.get("updatedAt"))
        closed_at = parse_datetime(pr.get("closedAt"))
        merged_at = parse_datetime(pr.get("mergedAt"))

        days_since_created = (now - created_at).days if created_at else None
        days_since_updated = (now - updated_at).days if updated_at else None
        time_to_close_hours = int((closed_at - created_at).total_seconds() / 3600) if closed_at and created_at else None
        time_to_merge_hours = int((merged_at - created_at).total_seconds() / 3600) if merged_at and created_at else None

        reviews = (pr.get("reviews") or {}).get("totalCount")
        comments = (pr.get("comments") or {}).get("totalCount")

        row = {
            "project_item_id": f"repo-{pr_number}",
            "pr_id": _str(pr.get("id")),
            "pr_number": pr_number,
            "title": _str(pr.get("title")),
            "url": _str(pr.get("url")),
            "state": _str(pr.get("state")),
            "body": _str(pr.get("body")),
            "body_text": _str(pr.get("bodyText")),
            "is_draft": 1 if pr.get("isDraft") else 0,
            "additions": pr.get("additions"),
            "deletions": pr.get("deletions"),
            "changed_files": pr.get("changedFiles"),
            "mergeable": pr.get("mergeable"),  # MERGEABLE, CONFLICTING, UNKNOWN
            "merged": 1 if pr.get("merged") else 0,
            "created_at": created_at,
            "updated_at": updated_at,
            "closed_at": closed_at,
            "merged_at": merged_at,
            "created_date": created_at.date() if created_at else None,
            "updated_date": (updated_at or created_at).date() if (updated_at or created_at) else None,
            "days_since_created": days_since_created,
            "days_since_updated": days_since_updated,
            "time_to_close_hours": time_to_close_hours,
            "time_to_merge_hours": time_to_merge_hours,
            "author_login": _str(author.get("login")),
            "author_url": _str(author.get("url")),
            "repository_name": _str((pr.get("repository") or {}).get("name")),
            "repository_url": _str((pr.get("repository") or {}).get("url")),
            "head_ref_name": pr.get("headRefName"),
            "base_ref_name": pr.get("baseRefName"),
            "assignees": json.dumps(assignees) if assignees else None,
            "labels": json.dumps(labels) if labels else None,
            "milestone": json.dumps(milestone_info) if milestone_info else None,
            "project_fields": json.dumps(pr_project) if pr_project else None,
            "review_decision": pr.get("reviewDecision"),
            "total_reviews_count": reviews,
            "total_comments_count": comments,
            "merged_by_login": merged_by.get("login"),
            "merged_by_url": merged_by.get("url"),
            "info": _build_info(pr),
            "exported_at": now,
        }
        rows.append(row)

    elapsed = time.time() - start_time
    print(f"Transformed {len(rows)} PRs (took {elapsed:.2f}s)")
    return rows


def create_pull_requests_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    """Create pull_requests table in YDB if it does not exist."""
    print(f"Creating table: {table_path}")
    ydb_wrapper.create_table(table_path, _build_create_table_sql(table_path))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export GitHub pull requests to YDB. Default: PRs updated in last 24h. Use --days N for last N days, --full for all."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Export all pull requests",
    )
    parser.add_argument(
        "--days",
        type=int,
        metavar="N",
        default=None,
        help="Export PRs updated in the last N days (e.g. --days 60). Default: 1 (last 24 hours).",
    )
    parser.add_argument(
        "--since-date",
        metavar="YYYY-MM-DD",
        help="Export PRs updated on or after this date (UTC). Use with --until-date for a range.",
    )
    parser.add_argument(
        "--until-date",
        metavar="YYYY-MM-DD",
        help="Export PRs updated before this date (UTC). Use with --since-date for a range.",
    )
    args = parser.parse_args()

    print("Starting GitHub pull requests export to YDB")
    script_start = time.time()

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("Error: YDB credentials check failed")
            return 1
        if "GITHUB_TOKEN" not in os.environ:
            print("Error: GITHUB_TOKEN is required")
            return 1

        table_path = ydb_wrapper.get_table_path("pull_requests")
        batch_size = 100

        try:
            create_pull_requests_table(ydb_wrapper, table_path)

            if args.full:
                updated_since = None
                updated_until = None
                print("Mode: full export (all PRs)")
            elif args.since_date or args.until_date:
                if not args.since_date or not args.until_date:
                    print("Error: --since-date and --until-date must be used together")
                    return 1
                try:
                    updated_since = datetime.fromisoformat(args.since_date + "T00:00:00+00:00")
                    updated_until = datetime.fromisoformat(args.until_date + "T00:00:00+00:00")
                except ValueError as e:
                    print(f"Error: invalid date format (use YYYY-MM-DD): {e}")
                    return 1
                if updated_since >= updated_until:
                    print("Error: --since-date must be before --until-date")
                    return 1
                print(f"Mode: export PRs updated in [{args.since_date}, {args.until_date})")
            else:
                updated_until = None
                days = args.days if args.days is not None else 1
                updated_since = datetime.now(timezone.utc) - timedelta(days=days)
                if days == 1:
                    print("Mode: export PRs updated in the last 24 hours")
                else:
                    print(f"Mode: export PRs updated in the last {days} days")

            pr_nodes = fetch_repository_pull_requests(
                ORG_NAME, REPO_NAME,
                updated_since=updated_since,
                updated_until=updated_until,
            )
            if not pr_nodes:
                print("No PRs to export")
                return 0

            rows = transform_pull_requests_for_ydb(pr_nodes)
            print(f"Uploading {len(rows)} PRs in batches of {batch_size}")

            ydb_wrapper.bulk_upsert_batches(table_path, rows, _build_column_types(), batch_size)
            print(f"Script completed successfully (total time: {time.time() - script_start:.2f}s)")
            return 0
        except Exception as e:
            print(f"Error: {e}")
            return 1


if __name__ == "__main__":
    exit(main())
