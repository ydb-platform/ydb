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
PROJECT_ID = None  # Optional: set to project number to fetch project V2 fields for PRs


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
) -> List[Dict[str, Any]]:
    """Fetch pull requests from repository. If updated_since is set, stop when all remaining PRs are older."""
    if updated_since:
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
            if updated_since:
                updated_at = parse_datetime(node.get('updatedAt'))
                if updated_at is None or updated_at < updated_since:
                    # Rest of page and all next pages are older; include only PRs we already have
                    has_next_page = False
                    break
            prs.append(node)
        else:
            # No break: full page was within range
            total = len(prs)
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


def get_project_fields_for_pull_requests(
    org_name: str,
    project_id: str,
    pr_numbers: List[int],
) -> Dict[int, Dict[str, Any]]:
    """Get project V2 field values for given PR numbers (content type PullRequest)."""
    if not project_id:
        return {}
    print(f"Fetching project fields for {len(pr_numbers)} PRs from project {project_id}...")
    start_time = time.time()
    project_fields: Dict[int, Dict[str, Any]] = {}
    pr_set = set(pr_numbers)
    has_next_page = True
    end_cursor = "null"

    query_template = """
    {
      organization(login: "%s") {
        projectV2(number: %s) {
          items(first: 1000, after: %s) {
            nodes {
              id
              content {
                ... on PullRequest { number }
              }
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
        q = query_template % (org_name, project_id, end_cursor)
        result = run_query(q)
        if not result or 'data' not in result:
            break
        items = result['data']['organization']['projectV2']['items']
        nodes = items.get('nodes') or []
        page_info = items.get('pageInfo') or {}
        for item in nodes:
            content = item.get('content')
            if not content or content.get('number') not in pr_set:
                continue
            pr_num = content['number']
            fields: Dict[str, Any] = {}
            for fv in item.get('fieldValues', {}).get('nodes', []):
                f = (fv.get('field') or {}).get('name')
                if not f:
                    continue
                key = f.lower()
                if 'name' in fv:
                    fields[key] = fv.get('name')
                elif 'text' in fv:
                    fields[key] = fv.get('text')
                elif 'number' in fv:
                    fields[key] = fv.get('number')
                elif 'date' in fv:
                    fields[key] = fv.get('date')
            project_fields[pr_num] = fields
        has_next_page = page_info.get('hasNextPage', False)
        end_cursor = f'"{page_info["endCursor"]}"' if page_info.get('endCursor') else "null"

    elapsed = time.time() - start_time
    print(f"Fetched project fields for {len(project_fields)} PRs (took {elapsed:.2f}s)")
    return project_fields


def transform_pull_requests_for_ydb(
    pr_nodes: List[Dict[str, Any]],
    project_fields: Optional[Dict[int, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Transform GraphQL PR nodes into YDB row dicts."""
    print("Transforming PRs for YDB...")
    start_time = time.time()
    if project_fields is None:
        project_fields = {}
    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for pr in pr_nodes:
        pr_number = pr.get('number')
        pr_project = project_fields.get(pr_number, {}) if pr_number else {}
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
            "pr_id": pr.get("id", ""),
            "pr_number": pr_number,
            "title": pr.get("title", ""),
            "url": pr.get("url", ""),
            "state": pr.get("state", ""),
            "body": pr.get("body", ""),
            "body_text": pr.get("bodyText", ""),
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
            "updated_date": updated_at.date() if updated_at else None,
            "days_since_created": days_since_created,
            "days_since_updated": days_since_updated,
            "time_to_close_hours": time_to_close_hours,
            "time_to_merge_hours": time_to_merge_hours,
            "author_login": author.get("login") or "",
            "author_url": author.get("url") or "",
            "repository_name": (pr.get("repository") or {}).get("name", ""),
            "repository_url": (pr.get("repository") or {}).get("url", ""),
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
            "info": None,
            "exported_at": now,
        }
        rows.append(row)

    elapsed = time.time() - start_time
    print(f"Transformed {len(rows)} PRs (took {elapsed:.2f}s)")
    return rows


def create_pull_requests_table(ydb_wrapper: YDBWrapper, table_path: str) -> None:
    """Create pull_requests table in YDB if it does not exist."""
    print(f"Creating table: {table_path}")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `project_item_id` Utf8 NOT NULL,
            `pr_id` Utf8 NOT NULL,
            `pr_number` Uint64 NOT NULL,
            `title` Utf8,
            `url` Utf8,
            `state` Utf8,
            `body` Utf8,
            `body_text` Utf8,
            `is_draft` Int NOT NULL,
            `additions` Uint64,
            `deletions` Uint64,
            `changed_files` Uint64,
            `mergeable` Utf8,
            `merged` Int NOT NULL,
            `created_at` Timestamp NOT NULL,
            `updated_at` Timestamp,
            `closed_at` Timestamp,
            `merged_at` Timestamp,
            `created_date` Date NOT NULL,
            `updated_date` Date NOT NULL,
            `days_since_created` Uint64,
            `days_since_updated` Uint64,
            `time_to_close_hours` Uint64,
            `time_to_merge_hours` Uint64,
            `author_login` Utf8,
            `author_url` Utf8,
            `repository_name` Utf8,
            `repository_url` Utf8,
            `head_ref_name` Utf8,
            `base_ref_name` Utf8,
            `assignees` Json,
            `labels` Json,
            `milestone` Json,
            `project_fields` Json,
            `review_decision` Utf8,
            `total_reviews_count` Uint64,
            `total_comments_count` Uint64,
            `merged_by_login` Utf8,
            `merged_by_url` Utf8,
            `info` Json,
            `exported_at` Timestamp NOT NULL,
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
    ydb_wrapper.create_table(table_path, create_sql)


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
                print("Mode: full export (all PRs)")
            else:
                days = args.days if args.days is not None else 1
                updated_since = datetime.now(timezone.utc) - timedelta(days=days)
                if days == 1:
                    print("Mode: export PRs updated in the last 24 hours")
                else:
                    print(f"Mode: export PRs updated in the last {days} days")

            pr_nodes = fetch_repository_pull_requests(ORG_NAME, REPO_NAME, updated_since=updated_since)
            if not pr_nodes:
                print("No PRs to export")
                return 0

            project_fields = {}
            if PROJECT_ID:
                pr_numbers = [p.get("number") for p in pr_nodes if p.get("number") is not None]
                if pr_numbers:
                    project_fields = get_project_fields_for_pull_requests(ORG_NAME, PROJECT_ID, pr_numbers)

            rows = transform_pull_requests_for_ydb(pr_nodes, project_fields)
            print(f"Uploading {len(rows)} PRs in batches of {batch_size}")

            column_types = (
                ydb.BulkUpsertColumns()
                .add_column("project_item_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("pr_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("pr_number", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("title", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("body", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("body_text", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("is_draft", ydb.OptionalType(ydb.PrimitiveType.Int32))
                .add_column("additions", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("deletions", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("changed_files", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("mergeable", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("merged", ydb.OptionalType(ydb.PrimitiveType.Int32))
                .add_column("created_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("updated_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("closed_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("merged_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("created_date", ydb.OptionalType(ydb.PrimitiveType.Date))
                .add_column("updated_date", ydb.OptionalType(ydb.PrimitiveType.Date))
                .add_column("days_since_created", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("days_since_updated", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("time_to_close_hours", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("time_to_merge_hours", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("author_login", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("author_url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("repository_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("repository_url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("head_ref_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("base_ref_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("assignees", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("labels", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("milestone", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("project_fields", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("review_decision", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("total_reviews_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("total_comments_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("merged_by_login", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("merged_by_url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("info", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("exported_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
            )
            ydb_wrapper.bulk_upsert_batches(table_path, rows, column_types, batch_size)
            print(f"Script completed successfully (total time: {time.time() - script_start:.2f}s)")
            return 0
        except Exception as e:
            print(f"Error: {e}")
            return 1


if __name__ == "__main__":
    exit(main())
