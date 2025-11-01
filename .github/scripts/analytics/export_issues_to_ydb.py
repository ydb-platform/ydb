#!/usr/bin/env python3

import os
import ydb
import configparser
import time
import json
from datetime import datetime, timezone, timedelta
import requests
from typing import List, Dict, Any, Optional
from ydb_wrapper import YDBWrapper

# Configuration
ORG_NAME = 'ydb-platform'
REPO_NAME = 'ydb'
PROJECT_ID = None #'45'  # Optional: set to None to skip project data

# YDB configuration is now handled by ydb_wrapper

def run_query(query: str, variables: Optional[Dict] = None) -> Dict[str, Any]:
    """Execute GraphQL query against GitHub API"""
    GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
    HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Content-Type": "application/json"}
    
    request = requests.post(
        'https://api.github.com/graphql', 
        json={'query': query, 'variables': variables}, 
        headers=HEADERS
    )
    
    if request.status_code == 200:
        response = request.json()
        if 'errors' in response:
            for error in response['errors']:
                print(f"GraphQL Error: {error.get('message', 'Unknown error')}")
                raise Exception(f"GraphQL Error: {error.get('message', 'Unknown error')}")
        return response
    else:
        raise Exception(f"Query failed with status {request.status_code}: {request.text}")

def get_last_update_time(ydb_wrapper: YDBWrapper, table_path: str) -> Optional[datetime]:
    """Get the latest updated_at timestamp from existing records"""
    try:
        query = f"SELECT MAX(updated_at) as max_updated_at FROM `{table_path}`"
        results = ydb_wrapper.execute_scan_query(query)
        
        if results and results[0]['max_updated_at']:
            # Convert timestamp to datetime
            timestamp = results[0]['max_updated_at']
            if isinstance(timestamp, int):
                # YDB timestamp is in microseconds
                return datetime.fromtimestamp(timestamp / 1000000, tz=timezone.utc)
            elif isinstance(timestamp, datetime):
                return timestamp
        return None

    except Exception as e:
        print(f"Warning: Could not get last update time: {e}")
        return None



def fetch_repository_issues(org_name: str = ORG_NAME, repo_name: str = REPO_NAME, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Fetch all issues from GitHub repository with comprehensive information"""
    if since:
        print(f"Fetching issues updated since {since.isoformat()} from repository {org_name}/{repo_name}...")
    else:
        print(f"Fetching all issues from repository {org_name}/{repo_name}...")
    start_time = time.time()
    
    issues = []
    has_next_page = True
    end_cursor = "null"
    
    # Convert datetime to GitHub API format if needed
    since_filter = ""
    if since:
        since_str = since.strftime('%Y-%m-%dT%H:%M:%SZ')
        since_filter = f', filterBy: {{since: "{since_str}"}}'
    
    repository_issues_query = """
    {
      organization(login: "%s") {
        repository(name: "%s") {
          issues(first: 100, after: %s, orderBy: {field: UPDATED_AT, direction: DESC}%s) {
            nodes {
              id
              number
              title
              url
              state
              body
              bodyText
              createdAt
              updatedAt
              closedAt
              author {
                login
                url
              }
              assignees(first: 10) {
                nodes {
                  login
                  url
                }
              }
              labels(first: 20) {
                nodes {
                  id
                  name
                  color
                  description
                }
              }
              milestone {
                id
                title
                url
                state
                dueOn
              }
              reactions {
                totalCount
              }
              comments {
                totalCount
              }
              repository {
                id
                name
                url
              }
              participants(first: 10) {
                totalCount
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      }
    }
    """
    
    total_fetched = 0
    while has_next_page:
        query = repository_issues_query % (org_name, repo_name, end_cursor, since_filter)
        result = run_query(query)
        
        if result and 'data' in result:
            repository_issues = result['data']['organization']['repository']['issues']
            current_batch = repository_issues['nodes']
            
            issues.extend(current_batch)
            total_fetched += len(current_batch)
            
            print(f"Fetched {len(current_batch)} issues from repository (total: {total_fetched})")
            
            page_info = repository_issues['pageInfo']
            has_next_page = page_info['hasNextPage']
            end_cursor = f'"{page_info["endCursor"]}"' if page_info['endCursor'] else "null"
        else:
            has_next_page = False
    
    elapsed = time.time() - start_time
    print(f"Fetched {len(issues)} issues total (took {elapsed:.2f}s)")
    return issues

def get_project_fields_for_issues(org_name: str, project_id: str, issue_numbers: List[int]) -> Dict[int, Dict[str, Any]]:
    """Get project fields for specific issues from GitHub project"""
    if not project_id:
        return {}
    
    print(f"Fetching project fields for {len(issue_numbers)} issues from project {project_id}...")
    start_time = time.time()
    
    project_fields = {}
    has_next_page = True
    end_cursor = "null"
    
    project_issues_query = """
    {
      organization(login: "%s") {
        projectV2(number: %s) {
          id
          title
          url
          items(first: 1000, after: %s) {
            nodes {
              id
              content {
                ... on Issue {
                  number
                }
              }
              fieldValues(first: 20) {
                nodes {
                  ... on ProjectV2ItemFieldSingleSelectValue {
                    field {
                      ... on ProjectV2SingleSelectField {
                        id
                        name
                      }
                    }
                    name
                    id
                    updatedAt
                  }
                  ... on ProjectV2ItemFieldTextValue {
                    field {
                      ... on ProjectV2Field {
                        id
                        name
                      }
                    }
                    text
                    id
                    updatedAt
                    creator {
                      login
                    }
                  }
                  ... on ProjectV2ItemFieldMilestoneValue {
                    field {
                      ... on ProjectV2Field {
                        id
                        name
                      }
                    }
                    milestone {
                      id
                      title
                    }
                  }
                  ... on ProjectV2ItemFieldDateValue {
                    field {
                      ... on ProjectV2Field {
                        id
                        name
                      }
                    }
                    date
                    updatedAt
                  }
                  ... on ProjectV2ItemFieldNumberValue {
                    field {
                      ... on ProjectV2Field {
                        id
                        name
                      }
                    }
                    number
                    updatedAt
                  }
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      }
    }
    """
    
    issue_numbers_set = set(issue_numbers)
    
    while has_next_page:
        query = project_issues_query % (org_name, project_id, end_cursor)
        result = run_query(query)
        
        if result and 'data' in result:
            project_items = result['data']['organization']['projectV2']['items']
            current_batch = project_items['nodes']
            
            for item in current_batch:
                content = item.get('content')
                if content and content.get('number') in issue_numbers_set:
                    issue_number = content['number']
                    
                    # Extract project field values
                    fields = {}
                    field_values = item.get('fieldValues', {}).get('nodes', [])
                    
                    for field_value in field_values:
                        field_name = field_value.get('field', {}).get('name', '')
                        if field_name:
                            if 'name' in field_value:  # SingleSelect
                                fields[field_name.lower()] = field_value.get('name')
                            elif 'text' in field_value:  # Text
                                fields[field_name.lower()] = field_value.get('text')
                            elif 'number' in field_value:  # Number
                                fields[field_name.lower()] = field_value.get('number')
                            elif 'date' in field_value:  # Date
                                fields[field_name.lower()] = field_value.get('date')
                            elif 'milestone' in field_value:  # Milestone
                                milestone = field_value.get('milestone', {})
                                fields[field_name.lower()] = milestone.get('title') if milestone else None
                    
                    project_fields[issue_number] = fields
            
            page_info = project_items['pageInfo']
            has_next_page = page_info['hasNextPage']
            end_cursor = f'"{page_info["endCursor"]}"' if page_info['endCursor'] else "null"
        else:
            has_next_page = False
    
    elapsed = time.time() - start_time
    print(f"Fetched project fields for {len(project_fields)} issues (took {elapsed:.2f}s)")
    return project_fields

def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse GitHub datetime string to datetime object"""
    if not dt_str:
        return None
    try:
        # GitHub returns ISO format with Z suffix
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None

# --- branch version helpers ---
def parse_branch(label):
    if label == 'main':
        return (0, 0, 0, 0, 0)  # main — всегда минимальный
    if label.startswith('prestable-'):
        parts = label.split('-')
        nums = [int(x) for x in parts[1:] if x.isdigit()]
        while len(nums) < 3:
            nums.append(0)
        return (1, *nums, 0)  # prestable < analytics < stable
    if label.startswith('stable-'):
        parts = label.split('-')
        nums = []
        analytics = 0
        for x in parts[1:]:
            if x.isdigit():
                nums.append(int(x))
            elif x == 'analytics':
                analytics = 1
        while len(nums) < 3:
            nums.append(0)
        if analytics:
            # analytics-лейбл: всегда меньше любого stable с числовым патчем, но больше prestable
            return (2, *nums, 0)  # analytics = 2
        else:
            return (3, *nums, 1)  # обычный stable = 3, всегда больше analytics
    return (-1, 0, 0, 0, 0)  # некорректные/другие — минимальные

def get_max_branch(branch_labels):
    best = None
    best_key = (-2, 0, 0, 0, 0)  # всегда меньше любого корректного branch
    for label in branch_labels:
        key = parse_branch(label)
        if key > best_key:
            best = label
            best_key = key
    return best

def transform_issues_for_ydb(issues: List[Dict[str, Any]], project_fields: Optional[Dict[int, Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Transform GitHub issues data for YDB storage"""
    print("Transforming issues data for YDB...")
    start_time = time.time()
    
    if project_fields is None:
        project_fields = {}
    
    transformed_issues = []
    
    for issue in issues:
        # Get project fields for this issue if available
        issue_number = issue.get('number')
        issue_project_fields = project_fields.get(issue_number, {}) if issue_number else {}
        
        # Extract labels
        labels = []
        branch_labels = []
        env = None
        priority = None
        area = None
        for label in issue.get('labels', {}).get('nodes', []):
            name = label.get('name', '')
            labels.append({
                'name': name,
                'color': label.get('color', ''),
                'description': label.get('description', '')
            })
            # branch detection (main, stable-*, prestable-*)
            if name == 'main' or name.startswith('stable-') or name.startswith('prestable-'):
                branch_labels.append(name)
            # env detection
            if name.startswith('env:'):
                env = name
            # priority detection
            if name.startswith('prio:'):
                priority = name
            # area detection
            if name.startswith('area/'):
                area = name
        branch = ';'.join(branch_labels) if branch_labels else None
        max_branch = get_max_branch(branch_labels) if branch_labels else None
        info = {'branch': branch, 'max_branch': max_branch, 'env': env, 'priority': priority, 'area': area}
        # Issue type from project fields
        issue_type = issue_project_fields.get('type') or issue_project_fields.get('Type')
        
        # Extract assignees
        assignees = []
        for assignee in issue.get('assignees', {}).get('nodes', []):
            assignees.append({
                'login': assignee.get('login', ''),
                'url': assignee.get('url', '')
            })
        
        # Extract milestone
        milestone = issue.get('milestone')
        milestone_info = None
        if milestone:
            milestone_info = {
                'title': milestone.get('title', ''),
                'url': milestone.get('url', ''),
                'state': milestone.get('state', ''),
                'due_on': milestone.get('dueOn')
            }
        
        # Extract author
        author = issue.get('author', {})
        author_info = {
            'login': author.get('login', '') if author else '',
            'url': author.get('url', '') if author else ''
        }
        
        # Parse timestamps
        created_at = parse_datetime(issue.get('createdAt'))
        updated_at = parse_datetime(issue.get('updatedAt'))
        closed_at = parse_datetime(issue.get('closedAt'))
        now = datetime.now(timezone.utc)
        
        is_in_project = bool(issue_project_fields)
        
        # Calculate time-based metrics
        days_since_created = 0
        days_since_updated = 0
        time_to_close_hours = 0
        
        if created_at:
            days_since_created = (now - created_at).days
        if updated_at:
            days_since_updated = (now - updated_at).days
        if closed_at and created_at:
            time_to_close_hours = int((closed_at - created_at).total_seconds() / 3600)
        
        # Build the record
        issue_record = {
            # Primary identifiers
            'project_item_id': f"repo-{issue.get('number', 0)}",
            'issue_id': issue.get('id', ''),
            'issue_number': issue.get('number', 0),
            
            # Core issue data
            'title': issue.get('title', ''),
            'url': issue.get('url', ''),
            'state': issue.get('state', ''),
            'body': issue.get('body', '') or '',
            'body_text': issue.get('bodyText', ''),
            
            # Time dimensions
            'created_at': created_at,
            'updated_at': updated_at,
            'closed_at': closed_at,
            'created_date': created_at.date() if created_at else None,
            'updated_date': updated_at.date() if updated_at else None,
            
            # User dimensions
            'author_login': author_info['login'],
            'author_url': author_info['url'],
            
            
            # Repository dimensions
            'repository_name': issue.get('repository', {}).get('name', ''),
            'repository_url': issue.get('repository', {}).get('url', ''),
            
            # Project dimensions
            'project_status': issue_project_fields.get('status'),
            'project_owner': issue_project_fields.get('owner'),
            'project_priority': issue_project_fields.get('priority'),
            'is_in_project': is_in_project,
            
            # Time-based metrics
            'days_since_created': days_since_created,
            'days_since_updated': days_since_updated,
            'time_to_close_hours': time_to_close_hours,
            
            # Complex data
            'assignees': json.dumps(assignees) if assignees else None,
            'labels': json.dumps(labels) if labels else None,
            'milestone': json.dumps(milestone_info) if milestone_info else None,
            'project_fields': json.dumps(issue_project_fields) if issue_project_fields else None,
            'info': json.dumps(info) if any(info.values()) else None,
            'issue_type': issue_type,
            
            # System fields
            'exported_at': now
        }
        
        transformed_issues.append(issue_record)
    
    elapsed = time.time() - start_time
    print(f"Transformed {len(transformed_issues)} issues (took {elapsed:.2f}s)")
    return transformed_issues

def create_issues_table(ydb_wrapper: YDBWrapper, table_path: str):
    """Create issues table in YDB optimized for BI"""
    print(f"Creating BI-optimized table: {table_path}")
    start_time = time.time()
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            -- Primary identifiers
            `project_item_id` Utf8 NOT NULL,
            `issue_id` Utf8 NOT NULL,
            `issue_number` Uint64 NOT NULL,
            
            -- Core issue data
            `title` Utf8,
            `url` Utf8,
            `state` Utf8,
            `body` Utf8,
            `body_text` Utf8,
            
            -- Time dimensions for BI (partitioning keys)
            `created_at` Timestamp NOT NULL,
            `updated_at` Timestamp,
            `closed_at` Timestamp,
            `created_date` Date NOT NULL,  -- Extracted date for better partitioning
            `updated_date` Date NOT NULL,  -- Extracted date for better partitioning
            
            -- User dimensions
            `author_login` Utf8,
            `author_url` Utf8,
            
            
            -- Repository dimensions
            `repository_name` Utf8,
            `repository_url` Utf8,
            
            -- Project dimensions (nullable for issues not in project)
            `project_status` Utf8,
            `project_owner` Utf8,
            `project_priority` Utf8,
            `is_in_project` Int NOT NULL,  -- Boolean flag for faster filtering
            
            -- Time-based metrics
            `days_since_created` Uint64,  -- Days since creation
            `days_since_updated` Uint64,  -- Days since last update
            `time_to_close_hours` Uint64,  -- Time to close in hours (if closed)
            
            -- Complex data (keep as JSON for detailed analysis)
            `assignees` Json,
            `labels` Json,
            `milestone` Json,
            `project_fields` Json,
            `info` Json,
            `issue_type` Utf8,
            
            -- System fields
            `exported_at` Timestamp NOT NULL,
            
            PRIMARY KEY (`created_date`, `issue_number`, `project_item_id`)
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
    
    elapsed = time.time() - start_time
    print(f"BI-optimized table created successfully (took {elapsed:.2f}s)")

def main():
    """Main function to export GitHub issues to YDB"""
    print("Starting GitHub issues export to YDB")
    script_start_time = time.time()
    
    # Initialize YDB wrapper with context manager for automatic cleanup
    with YDBWrapper() as ydb_wrapper:
        
        # Check credentials
        if not ydb_wrapper.check_credentials():
            print("Error: YDB credentials check failed")
            return 1
        
        # Check GitHub token
        if "GITHUB_TOKEN" not in os.environ:
            print("Error: Environment variable GITHUB_TOKEN is missing")
            return 1
        
        table_path = "github_data/issues"
        batch_size = 100
        
        try:
            # Create table if needed
            create_issues_table(ydb_wrapper, table_path)
            
            # Check if this is an incremental update
            last_update_time = get_last_update_time(ydb_wrapper, table_path)
            
            if last_update_time:
                print(f"Incremental update: fetching issues updated since {last_update_time.isoformat()}")
                # Add a small buffer to avoid missing issues due to timing issues
                since_time = last_update_time - timedelta(minutes=5)
            else:
                print("Full export: fetching all issues")
                since_time = None
            
            # Fetch issues from GitHub
            issues = fetch_repository_issues(ORG_NAME, REPO_NAME, since_time)
            
            if not issues:
                print("No issues fetched from GitHub")
                return 0
            
            # Get project fields if PROJECT_ID is specified
            project_fields = {}
            if PROJECT_ID:
                issue_numbers = []
                for issue in issues:
                    number = issue.get('number')
                    if number is not None and isinstance(number, int):
                        issue_numbers.append(number)
                if issue_numbers:
                    project_fields = get_project_fields_for_issues(ORG_NAME, PROJECT_ID, issue_numbers)
            
            # Transform issues for YDB
            transformed_issues = transform_issues_for_ydb(issues, project_fields)
            
            # Upsert issues in batches using bulk_upsert_batches
            print(f"Uploading {len(transformed_issues)} issues in batches of {batch_size}")
            upload_start_time = time.time()
            
            # Подготавливаем column_types один раз
            column_types = (
                ydb.BulkUpsertColumns()
                # Primary identifiers
                .add_column("project_item_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("issue_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("issue_number", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                
                # Core issue data
                .add_column("title", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("body", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("body_text", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                
                # Time dimensions
                .add_column("created_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("updated_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("closed_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                .add_column("created_date", ydb.OptionalType(ydb.PrimitiveType.Date))
                .add_column("updated_date", ydb.OptionalType(ydb.PrimitiveType.Date))
                
                # User dimensions
                .add_column("author_login", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("author_url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                
                # Repository dimensions
                .add_column("repository_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("repository_url", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                
                # Project dimensions
                .add_column("project_status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("project_owner", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("project_priority", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                .add_column("is_in_project", ydb.OptionalType(ydb.PrimitiveType.Int32))
                
                # Time-based metrics
                .add_column("days_since_created", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("days_since_updated", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                .add_column("time_to_close_hours", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                
                # Complex data
                .add_column("assignees", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("labels", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("milestone", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("project_fields", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("info", ydb.OptionalType(ydb.PrimitiveType.Json))
                .add_column("issue_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                
                # System fields
                .add_column("exported_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
            )
            
            ydb_wrapper.bulk_upsert_batches(table_path, transformed_issues, column_types, batch_size)
            
            upload_elapsed = time.time() - upload_start_time
            print(f"All issues uploaded (total upload time: {upload_elapsed:.2f}s)")
            
            # Show cluster info
            cluster_info = ydb_wrapper.get_cluster_info()
            print(f"\n📊 Export Summary:")
            print(f"   Session ID: {cluster_info.get('session_id')}")
            print(f"   Cluster Version: {cluster_info.get('version')}")
            print(f"   Statistics Status: {cluster_info.get('statistics_status')}")
            
            script_elapsed = time.time() - script_start_time
            print(f"Script completed successfully (total time: {script_elapsed:.2f}s)")
            
        except Exception as e:
            print(f"Error during execution: {e}")
            return 1
        
        return 0

if __name__ == "__main__":
    exit(main()) 