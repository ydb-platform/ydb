import os
import sys
import argparse
import json
import requests
import time
import importlib
from urllib.parse import quote_plus
import datetime
import re

# Import shared GitHub issue utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body, scan_to_utc_date
from mute_thresholds import get_thresholds
from manual_unmute_contract import (
    normalize_manual_unmute_status,
)


ORG_NAME = 'ydb-platform'
REPO_NAME = 'ydb'
PROJECT_ID = '45'
TEST_HISTORY_DASHBOARD = "https://datalens.yandex/4un3zdm0zcnyr"
CURRENT_TEST_HISTORY_DASHBOARD = "https://datalens.yandex/34xnbsom67hcq?"

# Github api (personal access token (classic)) token shoud have permitions to
# repo
# - repo:status
# - repo_deployment
# - public_repo
# admin:org
# project

GITHUB_MAX_BODY_LENGTH = 65000  # Setting slightly below 65536 to be safe
FAST_UNMUTE_AUTO_COMMENT_MARKER = "<!--fast-unmute-auto:v1-->"
MANUAL_RULES_SNAPSHOT_COMMENT_MARKER = "<!--manual-rules-snapshot:v1-->"
HUMAN_CLOSE_REOPEN_COMMENT_MARKER = "<!--human-close-reopen:v1-->"
UNMUTE_LIST_START_MARKER = "<!--unmute_list_start-->"
UNMUTE_LIST_END_MARKER = "<!--unmute_list_end-->"
# Machine-readable row state: separate line, hex-only inside <!-- ... --> (no "--" in payload; GFM hides reliably).
MUTE_CTRL_META_PREFIX = "mute_ctrl_meta:"
THRESHOLDS = get_thresholds()
MANUAL_FAST_UNMUTE_WINDOW_DAYS = THRESHOLDS["mute_manual_unmute_window_days"]
MANUAL_FAST_UNMUTE_MIN_PASSES = THRESHOLDS["mute_manual_unmute_min_passes"]
DEFAULT_UNMUTE_WINDOW_DAYS = THRESHOLDS["mute_default_unmute_window_days"]
DEFAULT_UNMUTE_MIN_PASSES = THRESHOLDS["mute_default_unmute_min_passes"]
MUTE_CONTROL_PART_MAX_TESTS = THRESHOLDS["control_comment_part_max_tests"]
REASON_NO_RUNS_DEFAULT_WINDOW = "no_runs_default_window"
REASON_STABLE_MANUAL_FAST_WINDOW = "stable_manual_fast_window"
REASON_STABLE_DEFAULT_WINDOW = "stable_default_window"
KNOWN_BOT_LOGINS = {
    "ydbot",
    "github-actions[bot]",
    "dependabot[bot]",
}
# Auto-enable manual path on human-close.
AUTO_ENABLE_MANUAL_ON_HUMAN_CLOSE = True

def truncate_issue_body(body):
    """Truncates issue body if it exceeds GitHub's maximum length.
    
    Args:
        body (str): The original issue body
        
    Returns:
        str: Truncated body if necessary, with a note about truncation
    """
    if len(body) <= GITHUB_MAX_BODY_LENGTH:
        return body
        
    truncation_message = "\n\n... [Content truncated due to length limitations] ..."
    available_length = GITHUB_MAX_BODY_LENGTH - len(truncation_message)
    
    # Find the last newline before the cutoff to avoid cutting in the middle of a line
    last_newline = body.rfind('\n', 0, available_length)
    if last_newline == -1:
        last_newline = available_length
        
    truncated_body = body[:last_newline] + truncation_message
    return truncated_body

def handle_github_errors(response):
    if 'errors' in response:
        for error in response['errors']:
            if error['type'] == 'INSUFFICIENT_SCOPES':
                print("Error: Insufficient Scopes")
                print("Message:", error['message'])
                raise Exception("Insufficient scopes. Please update your token's scopes.")
            # Handle other types of errors if necessary
            else:
                print("Unknown error type:", error.get('type', 'No type'))
                print("Message:", error.get('message', 'No message available'))
                raise Exception("GraphQL Error: " + error.get('message', 'Unknown error'))

def run_query(query, variables=None):
    GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not GITHUB_TOKEN:
        raise Exception("Neither GITHUB_TOKEN nor GH_TOKEN is set")
    HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Content-Type": "application/json"}
    max_attempts = 5
    timeout_seconds = 30

    for attempt in range(1, max_attempts + 1):
        try:
            request = requests.post(
                'https://api.github.com/graphql',
                json={'query': query, 'variables': variables},
                headers=HEADERS,
                timeout=timeout_seconds,
            )
        except requests.exceptions.RequestException as exc:
            if attempt == max_attempts:
                raise Exception(f"GitHub GraphQL request failed after {max_attempts} attempts: {exc}") from exc
            sleep_seconds = min(2 ** (attempt - 1), 10)
            print(
                f"run_query: transient request failure (attempt {attempt}/{max_attempts}), "
                f"retrying in {sleep_seconds}s: {exc}"
            )
            time.sleep(sleep_seconds)
            continue

        if request.status_code == 200:
            handle_github_errors(request.json())
            return request.json()

        should_retry = request.status_code in (429, 500, 502, 503, 504)
        if should_retry and attempt < max_attempts:
            sleep_seconds = min(2 ** (attempt - 1), 10)
            print(
                f"run_query: HTTP {request.status_code} (attempt {attempt}/{max_attempts}), "
                f"retrying in {sleep_seconds}s"
            )
            time.sleep(sleep_seconds)
            continue

        query_preview = query[:200].replace("\n", " ")
        raise Exception(
            f"Query failed with HTTP {request.status_code}. query_preview={query_preview}"
        )


def get_repository(org_name=ORG_NAME, repo_name=REPO_NAME):
    query = """
    {
      organization(login: "%s") {
        repository(name: "%s") {
          id
        }
      }
    }
    """ % (
        org_name,
        repo_name,
    )
    result = run_query(query)
    return result['data']['organization']['repository']


def get_project_v2_fields(org_name=ORG_NAME, project_id=PROJECT_ID):
    query_template = """
   {
      organization(login: "%s") {
        projectV2(number: %s) {
          id
          fields(first: 100) {
            nodes {
              ... on ProjectV2Field {
                id
                name
              }
              ... on ProjectV2SingleSelectField {
                id
                name
                options {
                  id
                  name
                }
              
              }
              
            }
          }
        }
      }
    }
    """
    query = query_template % (org_name, project_id)

    result = run_query(query)
    return (
        result['data']['organization']['projectV2']['id'],
        result['data']['organization']['projectV2']['fields']['nodes'],
    )


def create_and_add_issue_to_project(title, body, project_id=PROJECT_ID, org_name=ORG_NAME, state=None, owner=None):
    """Добавляет issue в проект.

    Args:
        title (str): Название issue.
        body (str): Содержимое issue.
        project_id (int): ID проекта.
        org_name (str): Имя организации.

    Returns:
        None
    """

    result = None
    # Truncate body if necessary
    body = truncate_issue_body(body)
    
    # Получаем ID полей "State" и "Owner"
    inner_project_id, project_fields = get_project_v2_fields(org_name, project_id)
    state_field_id = None
    owner_field_id = None
    for field in project_fields:
        if field.get('name'):
            if field['name'].lower() == "status":
                state_field_id = field['id']
                state_option_id = None
                if state:
                    for option in field['options']:
                        if option['name'].lower() == state.lower():
                            state_option_id = option['id']
                            break
            if field['name'].lower() == "owner":
                owner_field_id = field['id']
                owner_option_id = None
                if owner:
                    for option in field['options']:
                        if option['name'].lower() == owner.lower():
                            owner_option_id = option['id']
                            break

    if not state_field_id or not owner_field_id:
        raise Exception(f"Не найдены поля 'State' или 'Owner' в проекте {project_id}")
    # get repo
    repo = get_repository()
    # create issue
    query = """
    mutation ($repositoryId: ID!, $title: String!, $body: String!) {
      createIssue(input: {repositoryId: $repositoryId, title: $title, body: $body}) {
        issue {
          id,
          url
        }
      }
    }
    """
    variables = {"repositoryId": repo['id'], "title": title, "body": body}
    issue = run_query(query, variables)
    if not issue.get('errors'):
        print(f"Issue {title} created ")
    else:
        print(f"Error: Issue {title} not created ")
        return result
    issue_id = issue['data']['createIssue']['issue']['id']
    issue_url = issue['data']['createIssue']['issue']['url']

    query_add_to_project = """
     mutation ($projectId: ID!, $issueId: ID!) {
    addProjectV2ItemById(input: {projectId: $projectId, contentId: $issueId}) {
      item {
          id
        }
      }
    }
    """
    variables = {
        "projectId": inner_project_id,
        "issueId": issue_id,
    }
    result_add_to_project = run_query(query_add_to_project, variables)
    item_id = result_add_to_project['data']['addProjectV2ItemById']['item']['id']
    if not result_add_to_project.get('errors'):
        print(f"Issue {issue_url} added to project.")
    else:
        print(f"Error: Issue {title}: {issue_url} not added to project.")
        return result

    for field_name, filed_value, field_id, value_id in [
        ['state', state, state_field_id, state_option_id],
        ['owner', owner, owner_field_id, owner_option_id],
    ]:
        query_modify_fields = """
      mutation ($projectId: ID!, $itemId: ID!, $FieldId: ID!, $OptionId: String) {
        updateProjectV2ItemFieldValue(input: {
          projectId: $projectId,
          itemId: $itemId,
          fieldId: $FieldId,
          value: {
            singleSelectOptionId: $OptionId
          }
        }) {
          projectV2Item {
            id
          }
        }

      }
      """
        variables = {
            "projectId": inner_project_id,
            "itemId": item_id,
            "FieldId": field_id,
            "OptionId": value_id,
        }
        result_modify_field = run_query(query_modify_fields, variables)
        if not result_modify_field.get('errors'):
            print(f"Issue {title}: {issue_url} modified :{field_name} = {filed_value}")
        else:
            print(f"Error: Issue {title}: {issue_url}  not modified")
            return result
    result = {'issue_url': issue_url, 'owner': owner, 'title': title}
    return result


def fetch_all_issues(org_name=ORG_NAME, project_id=PROJECT_ID):
    issues = []
    has_next_page = True
    end_cursor = "null"
    page = 0

    project_issues_query = """
    {
      organization(login: "%s") {
        projectV2(number: %s) {
          id
          title
          items(first: 100, after: %s) {
            nodes {
              id
              content {
                ... on Issue {
                  id
                  number
                  title
                  url
                  state
                  body
                  createdAt
                  updatedAt
                  closedAt
                  timelineItems(
                    last: 50,
                    itemTypes: [CLOSED_EVENT, CONNECTED_EVENT, CROSS_REFERENCED_EVENT]
                  ) {
                    nodes {
                      __typename
                      ... on ClosedEvent {
                        actor {
                          __typename
                          ... on User {
                            login
                          }
                          ... on Bot {
                            login
                          }
                        }
                      }
                      ... on ConnectedEvent {
                        subject {
                          __typename
                          ... on PullRequest {
                            number
                            url
                            state
                            isDraft
                            repository {
                              nameWithOwner
                            }
                            labels(first: 20) {
                              nodes {
                                name
                              }
                            }
                          }
                        }
                      }
                      ... on CrossReferencedEvent {
                        source {
                          __typename
                          ... on PullRequest {
                            number
                            url
                            state
                            isDraft
                            repository {
                              nameWithOwner
                            }
                            labels(first: 20) {
                              nodes {
                                name
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              fieldValues(first: 20) {
                nodes {
                  ... on ProjectV2ItemFieldSingleSelectValue {
                    field {
                      ... on ProjectV2SingleSelectField {
                        name
                      }
                    }
                    name
                    id
                    updatedAt
                  }
                  ... on ProjectV2ItemFieldLabelValue {
                    labels(first: 20) {
                      nodes {
                        id
                        name
                      }
                    }
                  }
                  ... on ProjectV2ItemFieldTextValue {
                    text
                    id
                    updatedAt
                    creator {
                      url
                    }
                  }
                  ... on ProjectV2ItemFieldMilestoneValue {
                    milestone {
                      id
                    }
                  }
                  ... on ProjectV2ItemFieldRepositoryValue {
                    repository {
                      id
                      url
                    }
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
    while has_next_page:
        page += 1
        query = project_issues_query % (org_name, project_id, end_cursor)

        result = run_query(query)

        if result:
            project_items = result['data']['organization']['projectV2']['items']
            nodes = project_items['nodes']
            issues.extend(nodes)

            page_info = project_items['pageInfo']
            has_next_page = page_info['hasNextPage']
            end_cursor = f"\"{page_info['endCursor']}\"" if page_info['endCursor'] else "null"
            print(
                f"fetch_all_issues: page={page}, items={len(nodes)}, total={len(issues)}, has_next={has_next_page}",
                flush=True,
            )
        else:
            has_next_page = False

    return issues


def fetch_issues_by_numbers(issue_numbers, org_name=ORG_NAME, repo_name=REPO_NAME):
    """Fetch specific issues directly by number (without full project pagination)."""
    result_nodes = []
    for issue_number in sorted(set(int(x) for x in issue_numbers)):
        query = """
        {
          organization(login: "%s") {
            repository(name: "%s") {
              issue(number: %d) {
                id
                number
                title
                url
                state
                body
                createdAt
                updatedAt
                closedAt
                timelineItems(
                  last: 50,
                  itemTypes: [CLOSED_EVENT, CONNECTED_EVENT, CROSS_REFERENCED_EVENT]
                ) {
                  nodes {
                    __typename
                    ... on ClosedEvent {
                      actor {
                        __typename
                        ... on User { login }
                        ... on Bot { login }
                      }
                    }
                    ... on ConnectedEvent {
                      subject {
                        __typename
                        ... on PullRequest {
                          number
                          url
                          state
                          isDraft
                          repository { nameWithOwner }
                          labels(first: 20) { nodes { name } }
                        }
                      }
                    }
                    ... on CrossReferencedEvent {
                      source {
                        __typename
                        ... on PullRequest {
                          number
                          url
                          state
                          isDraft
                          repository { nameWithOwner }
                          labels(first: 20) { nodes { name } }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """ % (org_name, repo_name, issue_number)

        result = run_query(query)
        issue = (
            result.get("data", {})
            .get("organization", {})
            .get("repository", {})
            .get("issue")
        )
        if issue:
            result_nodes.append({"content": issue})
            print(f"fetch_issues_by_numbers: loaded issue #{issue_number}", flush=True)
        else:
            print(f"fetch_issues_by_numbers: issue #{issue_number} not found", flush=True)
    return result_nodes


def _extract_close_actor(content):
    timeline_nodes = (content or {}).get('timelineItems', {}).get('nodes', [])
    for node in reversed(timeline_nodes):
        if node.get('__typename') != 'ClosedEvent':
            continue
        actor = node.get('actor') or {}
        return actor.get('login'), actor.get('__typename')
    return None, None


def _is_bot_actor(login, actor_type):
    if actor_type == 'Bot':
        return True
    if not login:
        return True
    login_l = login.lower()
    if login_l in KNOWN_BOT_LOGINS:
        return True
    return login_l.endswith('[bot]')


def _extract_unmuted_tests_from_comment(comment_body):
    if not comment_body:
        return set()

    payload = comment_body
    if UNMUTE_LIST_START_MARKER in comment_body and UNMUTE_LIST_END_MARKER in comment_body:
        start_idx = comment_body.find(UNMUTE_LIST_START_MARKER) + len(UNMUTE_LIST_START_MARKER)
        end_idx = comment_body.find(UNMUTE_LIST_END_MARKER)
        payload = comment_body[start_idx:end_idx]

    tests = set()
    for line in payload.split('\n'):
        line = line.strip()
        if line.startswith('- Test '):
            tests.add(line[len('- Test '):].replace(' unmuted', ''))
    return tests


def _collect_issue_unmuted_tests(issue_id):
    unmuted = set()
    for comment in get_issue_comments(issue_id):
        unmuted.update(_extract_unmuted_tests_from_comment(comment))
    return unmuted


def _build_unmute_comment(header, tests):
    tests_payload = "\n".join(f"- Test {test}" for test in sorted(tests))
    return (
        f"{header}\n"
        f"{UNMUTE_LIST_START_MARKER}\n"
        f"{tests_payload}\n"
        f"{UNMUTE_LIST_END_MARKER}"
    )


def _extract_linked_development_pull_requests(content):
    timeline_nodes = (content or {}).get('timelineItems', {}).get('nodes', [])
    repo_full_name = f"{ORG_NAME}/{REPO_NAME}".lower()
    linked_prs = {}

    for node in timeline_nodes:
        pr = None
        if node.get('__typename') == 'ConnectedEvent':
            subject = node.get('subject') or {}
            if subject.get('__typename') == 'PullRequest':
                pr = subject
        elif node.get('__typename') == 'CrossReferencedEvent':
            source = node.get('source') or {}
            if source.get('__typename') == 'PullRequest':
                pr = source

        if not pr:
            continue

        pr_repo = ((pr.get('repository') or {}).get('nameWithOwner') or '').lower()
        if pr_repo and pr_repo != repo_full_name:
            continue

        if pr.get('isDraft'):
            continue

        if pr.get('state') not in {'OPEN', 'MERGED'}:
            continue

        if _pr_has_label(pr, "mute-unmute"):
            continue

        number = pr.get('number')
        if number is None:
            continue
        linked_prs[number] = pr

    return [linked_prs[number] for number in sorted(linked_prs)]


def _pr_has_label(pr, label_name):
    target = (label_name or "").strip().lower()
    if not target:
        return False
    label_nodes = ((pr or {}).get("labels") or {}).get("nodes") or []
    for node in label_nodes:
        name = (node or {}).get("name")
        if isinstance(name, str) and name.strip().lower() == target:
            return True
    return False


def _has_fast_unmute_comment(comments):
    return any(FAST_UNMUTE_AUTO_COMMENT_MARKER in (comment or '') for comment in comments)


def _build_fast_unmute_comment(issue_number, closer_login, linked_prs):
    linked_pr_lines = "\n".join(
        f"- #{pr.get('number')} ({pr.get('state')}): {pr.get('url')}" for pr in linked_prs
    )
    return (
        f"{FAST_UNMUTE_AUTO_COMMENT_MARKER}\n"
        f"Fast-unmute flow enabled for issue #{issue_number}.\n\n"
        "Reason:\n"
        f"- issue was closed manually by @{closer_login}\n"
        "- linked PR found in Development context\n\n"
        "Linked PRs:\n"
        f"{linked_pr_lines}\n\n"
        "Result:\n"
        f"- tests from this issue become eligible for {MANUAL_FAST_UNMUTE_WINDOW_DAYS}-day unmute window in mute update flow"
    )


def _has_human_close_reopen_comment(comments):
    return any(HUMAN_CLOSE_REOPEN_COMMENT_MARKER in (comment or "") for comment in comments)


def _build_human_close_reopen_comment(issue_number, active_tests):
    shown_tests = sorted(set(active_tests))[:10]
    test_lines = "\n".join(f"- `{name}`" for name in shown_tests)
    extra = ""
    if len(active_tests) > len(shown_tests):
        extra = f"\n- ... and {len(active_tests) - len(shown_tests)} more"
    return (
        f"{HUMAN_CLOSE_REOPEN_COMMENT_MARKER}\n"
        f"Issue #{issue_number} was closed manually, but there are still muted tests tracked by this issue.\n\n"
        "Bot actions applied:\n"
        "- issue reopened to keep tracking explicit\n"
        "- project status moved to `In Progress`\n"
        f"- manual-fast rules enabled for remaining active tests (window={MANUAL_FAST_UNMUTE_WINDOW_DAYS}d, "
        f"pass_count>{MANUAL_FAST_UNMUTE_MIN_PASSES}, fail+mute=0)\n\n"
        f"Remaining active tests ({len(active_tests)}):\n"
        f"{test_lines}{extra}"
    )


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def _parse_iso8601(value):
    if not value:
        return None
    s = value.strip()
    normalized = s.replace('Z', '+00:00')
    try:
        dt = datetime.datetime.fromisoformat(normalized)
    except ValueError:
        dt = None
    if dt is None and 'T' in s and len(s) >= 19:
        try:
            dt = datetime.datetime.strptime(s[:19], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            dt = None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _isoformat_z(dt_value):
    return dt_value.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _normalize_requested_at(value):
    dt = _parse_iso8601(value)
    return _isoformat_z(dt) if dt else ""


def _split_tests_into_chunks(test_names):
    names = sorted(set(test_names))
    if not names:
        return [[]]
    return [names[i:i + MUTE_CONTROL_PART_MAX_TESTS] for i in range(0, len(names), MUTE_CONTROL_PART_MAX_TESTS)]


def _control_part_start(part_idx, part_total):
    return f"<!-- mute_control_v1:start part:{part_idx}/{part_total} -->"


def _control_part_end():
    return "<!-- mute_control_v1:end -->"


def _extract_mute_control_payload(comment_body):
    """Text between canonical mute control start/end markers."""
    if not comment_body:
        return None
    m_new = re.search(r"<!--\s*mute_control_v1:start\s+part:\d+/\d+\s*-->", comment_body)
    if not m_new:
        return None
    m_end = re.search(r"<!--\s*mute_control_v1:end\s*-->", comment_body[m_new.end() :])
    if not m_end:
        return None
    return comment_body[m_new.end() : m_new.end() + m_end.start()]


def _render_control_comment(issue_number, part_idx, part_total, items):
    lines = [
        _control_part_start(part_idx, part_total),
        "### Mute control list (managed by bot)",
        f"Issue #{issue_number} | part {part_idx}/{part_total}",
        "",
        f"1. **Default path.** This issue will close automatically once **every** test here has been stable (green in CI, no new failures) for **{DEFAULT_UNMUTE_WINDOW_DAYS}** full days under the normal mute rules.",
        f"2. **You fixed only some tests.** Tick **`[x]`** next to those tests. That opts them into a **faster path** immediately: use a **{MANUAL_FAST_UNMUTE_WINDOW_DAYS}-day** short window with pass/fail criteria from config.",
        f"3. **You fixed all tests.** You can simply **close this issue yourself** (not via a bot). The bot will treat every still-active test the same as in (2): immediate **{MANUAL_FAST_UNMUTE_WINDOW_DAYS}-day** short window with configured criteria.",
        "",
    ]

    for test_name in sorted(items):
        item = items[test_name]
        requested = bool(item.get('requested'))
        state = item.get('state', 'active')
        requested_at = item.get('requested_at')
        status = item.get('status') or 'idle'
        reason = item.get('reason')
        resolved_at = item.get('resolved_at')
        check = "x" if requested and state == 'active' else " "

        if state == 'resolved':
            reason_text = reason or REASON_STABLE_DEFAULT_WINDOW
            reason_display = _format_unmuted_reason_for_comment(reason_text)
            meta_hex = _encode_mute_control_line_meta(
                "resolved",
                "resolved",
                "",
                reason_text,
                resolved_at or "",
            )
            lines.append(f"- [{check}] ~~`{test_name}`~~ - unmuted: {reason_display}")
            lines.append(f"  <!-- {MUTE_CTRL_META_PREFIX}{meta_hex} -->")
            continue

        meta_hex = _encode_mute_control_line_meta(
            "active",
            status,
            requested_at or "",
            "",
            "",
        )
        if requested:
            waiting_text = (
                f"muted: waiting test will be stable {MANUAL_FAST_UNMUTE_WINDOW_DAYS} days "
                f"and pass_count>{MANUAL_FAST_UNMUTE_MIN_PASSES} (fail+mute=0)"
            )
        else:
            waiting_text = (
                f"muted: waiting test will be stable {DEFAULT_UNMUTE_WINDOW_DAYS} days "
                f"and pass_count>{DEFAULT_UNMUTE_MIN_PASSES} (fail+mute=0)"
            )
        lines.append(f"- [{'x' if requested else ' '}] `{test_name}` - {waiting_text}")
        lines.append(f"  <!-- {MUTE_CTRL_META_PREFIX}{meta_hex} -->")

    lines.extend(
        [
            "",
            "---",
            "**Legend**",
            f"- `[ ]` — default **{DEFAULT_UNMUTE_WINDOW_DAYS}-day** stability path.",
            f"- `[x]` — short **{MANUAL_FAST_UNMUTE_WINDOW_DAYS}-day** path starts immediately.",
            "- ~~Strikethrough~~ — test already unmuted or removed from this issue.",
            "",
            "_Each test may have a hidden metadata line underneath (for automation only)._",
            _control_part_end(),
        ]
    )
    return "\n".join(lines)


def _format_unmuted_reason_for_comment(reason_text):
    if reason_text == REASON_STABLE_MANUAL_FAST_WINDOW:
        return f"manual, stable {MANUAL_FAST_UNMUTE_WINDOW_DAYS} days"
    if reason_text == REASON_STABLE_DEFAULT_WINDOW:
        return f"stable {DEFAULT_UNMUTE_WINDOW_DAYS} days"
    if reason_text == REASON_NO_RUNS_DEFAULT_WINDOW:
        return f"no runs, stable {DEFAULT_UNMUTE_WINDOW_DAYS} days"
    return reason_text or f"stable {DEFAULT_UNMUTE_WINDOW_DAYS} days"


def _build_test_history_link(test_name, branch):
    encoded = quote_plus(f"__in_{test_name}")
    return (
        f"{CURRENT_TEST_HISTORY_DASHBOARD}full_name={encoded}"
        f"&branch={branch}"
        "&SHOW_ONLY_BRANCH_RUN=true"
    )


def _extract_summary_history_map(issue_body):
    if not issue_body:
        return {}

    start_marker = "**Summary history:**"
    end_marker = "**Test run history:**"
    start_idx = issue_body.find(start_marker)
    if start_idx < 0:
        return {}

    payload = issue_body[start_idx + len(start_marker) :]
    end_idx = payload.find(end_marker)
    if end_idx >= 0:
        payload = payload[:end_idx]

    summary_by_test = {}
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        test_name, history = line.split(":", 1)
        test_key = test_name.strip().lstrip("-").strip()
        history_value = history.strip()
        if test_key and history_value:
            summary_by_test[test_key] = history_value
    return summary_by_test


def _is_stable_state_name(state_name):
    normalized = (state_name or "").strip().lower()
    if not normalized:
        return False
    if "fail" in normalized or "flaky" in normalized:
        return False
    return ("pass" in normalized) or ("stable" in normalized)


def _is_stable_daily_row(row):
    fail_count = row.get("fail_count")
    pass_count = row.get("pass_count")
    try:
        if fail_count is not None and int(fail_count) > 0:
            return False
        if pass_count is not None and int(pass_count) > 0:
            return True
    except (TypeError, ValueError):
        pass
    return _is_stable_state_name(row.get("state"))


def _format_date_window_label(raw_date_window):
    day = scan_to_utc_date(raw_date_window)
    if day is None:
        return "??-??"
    return day.strftime("%m-%d")


def _load_daily_timeline_from_ydb(test_names, branch, build_type):
    names = sorted(set(test_names or []))
    if not names:
        return {}

    escaped_names = ", ".join("'" + name.replace("'", "''") + "'" for name in names)
    days_window = int(max(DEFAULT_UNMUTE_WINDOW_DAYS, MANUAL_FAST_UNMUTE_WINDOW_DAYS))

    try:
        analytics_dir = os.path.join(os.path.dirname(__file__), "..", "analytics")
        if analytics_dir not in sys.path:
            sys.path.append(analytics_dir)
        ydb_wrapper_module = importlib.import_module("ydb_wrapper")
        YDBWrapper = getattr(ydb_wrapper_module, "YDBWrapper")
    except Exception as exc:
        print(f"manual-rules-snapshot: failed to import YDBWrapper: {exc}")
        return {}

    query = f"""
SELECT
    full_name,
    date_window,
    state,
    pass_count,
    fail_count
FROM `{{tests_monitor_table}}`
WHERE branch = '{branch}'
  AND build_type = '{build_type}'
  AND date_window >= CurrentUtcDate() - {days_window}*Interval("P1D")
  AND full_name IN ({escaped_names})
"""

    try:
        with YDBWrapper() as ydb_wrapper:
            if not ydb_wrapper.check_credentials():
                return {}
            tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor")
            rows = ydb_wrapper.execute_scan_query(
                query.format(tests_monitor_table=tests_monitor_table),
                query_name=f"manual_rules_snapshot_timeline_{branch}_{build_type}",
            )
    except Exception as exc:
        print(f"manual-rules-snapshot: failed to load timeline from YDB: {exc}")
        return {}

    grouped = {}
    for row in rows:
        full_name = row.get("full_name")
        if not full_name:
            continue
        grouped.setdefault(full_name, []).append(row)

    for full_name in grouped:
        grouped[full_name].sort(
            key=lambda item: (
                scan_to_utc_date(item.get("date_window")) or datetime.date.min,
                str(item.get("state") or ""),
            )
        )
    return grouped


def _ascii_stability_from_rows(rows, highlight_tail_points=0, max_points=None):
    if not rows:
        return "n/a"

    if max_points is not None and int(max_points) > 0:
        rows = rows[-int(max_points):]

    highlight_tail_points = max(int(highlight_tail_points or 0), 0)
    highlight_from = len(rows) - highlight_tail_points
    tokens = []
    for idx, row in enumerate(rows):
        marker = "+" if _is_stable_daily_row(row) else "-"
        day = _format_date_window_label(row.get("date_window"))
        token = f"{day}:{marker}"
        if highlight_tail_points and idx >= highlight_from:
            token = f"[{token}]"
        tokens.append(token)
    return " ".join(tokens)


def _timeline_ok_ratio(rows, rule_window_days):
    window = max(int(rule_window_days or 0), 1)
    if not rows:
        return f"0/{window}"
    window_rows = rows[-window:]
    ok_days = sum(1 for row in window_rows if _is_stable_daily_row(row))
    return f"{ok_days}/{window}"


def _rule_description_for_item(item):
    state = item.get("state", "active")
    if state == "resolved":
        reason = item.get("reason") or "resolved"
        return f"resolved ({reason})"

    requested = bool(item.get("requested"))
    if requested:
        return (
            f"manual-fast window={MANUAL_FAST_UNMUTE_WINDOW_DAYS}d, "
            f"criteria: pass_count>{MANUAL_FAST_UNMUTE_MIN_PASSES} and fail+mute=0"
        )

    return (
        f"default window={DEFAULT_UNMUTE_WINDOW_DAYS}d, "
        f"criteria: pass_count>{DEFAULT_UNMUTE_MIN_PASSES} and fail+mute=0"
    )


def _is_manual_rule_active(item):
    return bool((item or {}).get("state") == "active" and (item or {}).get("requested"))


def _resolved_window_days(item):
    reason = ((item or {}).get("reason") or "").strip()
    if reason == REASON_STABLE_MANUAL_FAST_WINDOW:
        return int(MANUAL_FAST_UNMUTE_WINDOW_DAYS)
    return int(DEFAULT_UNMUTE_WINDOW_DAYS)


def _rule_window_days_for_item(item):
    state = (item or {}).get("state", "active")
    if state == "resolved":
        return _resolved_window_days(item)
    if _is_manual_rule_active(item):
        return int(MANUAL_FAST_UNMUTE_WINDOW_DAYS)
    return int(DEFAULT_UNMUTE_WINDOW_DAYS)


def _rule_code_for_item(item):
    state = (item or {}).get("state", "active")
    if state == "resolved":
        return f"U{_resolved_window_days(item)}"
    return f"M{_rule_window_days_for_item(item)}"


def _md_escape_cell(value):
    return str(value).replace("|", "\\|")


def _display_test_name(full_name):
    if not full_name:
        return ""
    short = str(full_name).rsplit("/", 1)[-1]
    if ".py." in short:
        short = short.split(".py.", 1)[1]
    return short


def _build_manual_rules_snapshot_comment(issue_number, branch, build_type, tests, control_items, ydb_timelines):
    now_str = _isoformat_z(_utc_now())
    lines = [
        MANUAL_RULES_SNAPSHOT_COMMENT_MARKER,
        "### Manual-unmute rules snapshot (managed by bot)",
        f"Issue #{issue_number}",
        f"Generated at: {now_str}",
        "",
        (
            "Config (mute_thresholds.json): "
            f"default_window_days={DEFAULT_UNMUTE_WINDOW_DAYS}, default_min_passes={DEFAULT_UNMUTE_MIN_PASSES}; "
            f"manual_window_days={MANUAL_FAST_UNMUTE_WINDOW_DAYS}, manual_min_passes={MANUAL_FAST_UNMUTE_MIN_PASSES} "
            "(criteria uses pass_count > min_passes, fail+mute=0)"
        ),
        (
            f"Timeline highlighting: for tests on manual-fast path, last {MANUAL_FAST_UNMUTE_WINDOW_DAYS} day points "
            "are wrapped in []"
        ),
        "",
        "| TEST | RULE | TIMELINE | HISTORY |",
        "|---|---|---|---|",
    ]

    for test_name in sorted(set(tests)):
        item = control_items.get(test_name) or {}
        short_name = _display_test_name(test_name)
        rule = _rule_code_for_item(item)
        manual_active = _is_manual_rule_active(item)
        rule_window_days = _rule_window_days_for_item(item)
        full_rows = ydb_timelines.get(test_name, [])
        display_days = max(MANUAL_FAST_UNMUTE_WINDOW_DAYS + 2, 4) if manual_active else min(max(DEFAULT_UNMUTE_WINDOW_DAYS, 5), 7)
        visible_rows = full_rows[-display_days:] if full_rows else []
        ascii_timeline = _ascii_stability_from_rows(
            full_rows,
            highlight_tail_points=MANUAL_FAST_UNMUTE_WINDOW_DAYS if manual_active else 0,
            max_points=display_days,
        )
        ratio = _timeline_ok_ratio(visible_rows, rule_window_days)
        history_link = _build_test_history_link(test_name, branch)
        lines.append(
            "| "
            f"`{_md_escape_cell(short_name)}` | "
            f"{_md_escape_cell(rule)} | "
            f"`{_md_escape_cell(ascii_timeline)} · ok={_md_escape_cell(ratio)}` | "
            f"[🔗]({_md_escape_cell(history_link)}) |"
        )

    lines.extend(
        [
            "",
            "Legend:",
            (
                f"- `M{DEFAULT_UNMUTE_WINDOW_DAYS}` / `M{MANUAL_FAST_UNMUTE_WINDOW_DAYS}` = muted status "
                f"(default {DEFAULT_UNMUTE_WINDOW_DAYS}d: pass_count>{DEFAULT_UNMUTE_MIN_PASSES}, fail+mute=0; "
                f"manual {MANUAL_FAST_UNMUTE_WINDOW_DAYS}d: pass_count>{MANUAL_FAST_UNMUTE_MIN_PASSES}, fail+mute=0)."
            ),
            (
                f"- `U{DEFAULT_UNMUTE_WINDOW_DAYS}` / `U{MANUAL_FAST_UNMUTE_WINDOW_DAYS}` = unmuted status "
                "after corresponding window criteria are met."
            ),
            "- Timeline: `+` stable day, `-` unstable day, `[]` highlighted manual window tail.",
            "- `ok=a/b` = stable days in active rule window; `🔗` opens dashboard history.",
        ]
    )

    return "\n".join(lines)


def _encode_mute_control_line_meta(state, status, requested_at, reason, resolved_at):
    payload = {
        "state": state,
        "status": status,
        "requested_at": requested_at or "",
        "reason": reason or "",
        "resolved_at": resolved_at or "",
    }
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8").hex()


def _decode_mute_control_line_meta(hex_blob):
    if not hex_blob or not re.fullmatch(r"[0-9a-fA-F]*", hex_blob.strip()):
        return None
    try:
        raw = bytes.fromhex(hex_blob.strip())
        return json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _parse_control_items(comment_body):
    payload = _extract_mute_control_payload(comment_body)
    if payload is None:
        return {}
    lines_list = payload.split("\n")
    items = {}
    i = 0
    meta_hex_re = re.compile(
        rf"<!--\s*{re.escape(MUTE_CTRL_META_PREFIX)}([0-9a-fA-F]+)\s*-->",
        re.IGNORECASE,
    )
    while i < len(lines_list):
        stripped = lines_list[i].strip()
        if not stripped.startswith("- ["):
            i += 1
            continue
        m = re.search(r"`([^`]+)`", stripped)
        if not m:
            i += 1
            continue
        test_name = m.group(1).strip()
        requested = stripped.startswith("- [x]") or stripped.startswith("- [X]")

        if i + 1 >= len(lines_list):
            i += 1
            continue
        mhex = meta_hex_re.fullmatch(lines_list[i + 1].strip())
        if not mhex:
            i += 1
            continue
        meta_dict = _decode_mute_control_line_meta(mhex.group(1))
        if meta_dict is None:
            i += 2
            continue

        status = normalize_manual_unmute_status(
            meta_dict.get("status") or "",
            requested=requested,
        )
        items[test_name] = {
            "requested": requested,
            "state": meta_dict.get("state") or "active",
            "status": status,
            "reason": meta_dict.get("reason") or "",
            "requested_at": _normalize_requested_at(meta_dict.get("requested_at") or ""),
            "resolved_at": _normalize_requested_at(meta_dict.get("resolved_at") or ""),
        }
        i += 2
    return items


def _fetch_issue_comment_nodes(issue_id):
    query = """
    query ($issueId: ID!, $after: String) {
      node(id: $issueId) {
        ... on Issue {
          comments(first: 100, after: $after) {
            nodes {
              id
              body
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

    all_nodes = []
    after = None
    has_next_page = True

    while has_next_page:
        variables = {
            "issueId": issue_id,
            "after": after,
        }
        result = run_query(query, variables)
        comments = result.get('data', {}).get('node', {}).get('comments', {}) or {}
        nodes = comments.get('nodes') or []
        all_nodes.extend(nodes)
        page_info = comments.get('pageInfo') or {}
        has_next_page = bool(page_info.get('hasNextPage'))
        after = page_info.get('endCursor')

    return all_nodes


def get_issue_comments_with_ids(issue_id):
    nodes = _fetch_issue_comment_nodes(issue_id)
    return [{'id': node.get('id'), 'body': node.get('body', '')} for node in nodes if node.get('id')]


def edit_issue_comment(comment_id, comment):
    query = """
    mutation ($commentId: ID!, $body: String!) {
      updateIssueComment(input: {id: $commentId, body: $body}) {
        issueComment {
          id
        }
      }
    }
    """
    variables = {"commentId": comment_id, "body": comment}
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Updated comment {comment_id}")
    else:
        print(f"Error: Failed to update comment {comment_id}")


def _list_control_comments(issue_id):
    comments = get_issue_comments_with_ids(issue_id)
    return [
        comment
        for comment in comments
        if _comment_body_has_mute_control_start(comment.get("body") or "")
    ]


def _upsert_marked_comment(issue_id, marker, body):
    comments = get_issue_comments_with_ids(issue_id)
    marked = [comment for comment in comments if marker in (comment.get("body") or "")]
    if marked:
        target = marked[-1]
        if (target.get("body") or "") != body:
            edit_issue_comment(target["id"], body)
        return
    add_issue_comment(issue_id, body)


def _comment_body_has_mute_control_start(body):
    if not body:
        return False
    return bool(re.search(r"<!--\s*mute_control_v1:start\s+part:\d+/\d+\s*-->", body))


def _derive_resolution_reason(test_name, had_manual_request, stable_unmute_candidates, delete_candidates):
    if test_name in delete_candidates:
        return REASON_NO_RUNS_DEFAULT_WINDOW
    if test_name in stable_unmute_candidates:
        return REASON_STABLE_MANUAL_FAST_WINDOW if had_manual_request else REASON_STABLE_DEFAULT_WINDOW
    return REASON_STABLE_DEFAULT_WINDOW


def collect_manual_unmute_request_rows(
    branch='main',
    build_type='relwithdebinfo',
    stable_unmute_candidates=None,
    delete_candidates=None,
    require_non_bot_close_actor=True,
    require_linked_development_pr=False,
    add_auto_comment=True,
    updated_since_hours=None,
    sync_issue_comments=True,
    issue_numbers=None,
    post_manual_rules_snapshot_comment=False,
):
    """Collect manual-unmute request rows and fast-unmute overrides from control comments."""
    stable_unmute_candidates = stable_unmute_candidates or set()
    delete_candidates = delete_candidates or set()

    overrides = []
    request_rows = []
    issue_numbers_set = set(int(x) for x in issue_numbers) if issue_numbers else None
    print(
        "collect_manual_unmute_request_rows: "
        f"start fetch issues (project={PROJECT_ID}, updated_since_hours={updated_since_hours}, "
        f"issue_numbers={sorted(issue_numbers_set) if issue_numbers_set else 'all'})",
        flush=True,
    )
    if issue_numbers_set:
        issues = fetch_issues_by_numbers(issue_numbers_set, ORG_NAME, REPO_NAME)
    else:
        issues = fetch_all_issues(ORG_NAME, PROJECT_ID)
    print(
        f"collect_manual_unmute_request_rows: fetched issues={len(issues)}",
        flush=True,
    )
    stats = {
        'issues_total': 0,
        'issues_recent_match': 0,
        'issues_closed': 0,
        'issues_non_bot_closed': 0,
        'issues_with_linked_pr': 0,
        'issues_branch_match': 0,
        'overrides_total': 0,
        'issues_control_source': 0,
        'issues_legacy_source': 0,
        'issues_reopened_human_close': 0,
        'issues_set_in_progress': 0,
    }

    status_field_id = None
    in_progress_option_id = None
    if sync_issue_comments and AUTO_ENABLE_MANUAL_ON_HUMAN_CLOSE:
        try:
            status_field_id, option_ids = get_project_status_option_ids()
            in_progress_option_id = option_ids.get('in progress')
        except Exception as exc:
            print(f"collect_manual_unmute_request_rows: warning failed to load project status options: {exc}")

    now = _utc_now()
    updated_since_cutoff = None
    if updated_since_hours is not None and updated_since_hours > 0:
        updated_since_cutoff = now - datetime.timedelta(hours=int(updated_since_hours))
    total_issues = len(issues)
    for idx, issue in enumerate(issues, start=1):
        stats['issues_total'] += 1
        content = issue.get('content') or {}
        project_item_id = issue.get('id')
        issue_number = content.get('number')

        if idx == 1 or idx % 50 == 0 or idx == total_issues:
            print(
                f"collect_manual_unmute_request_rows: progress {idx}/{total_issues}, "
                f"current_issue={issue_number}",
                flush=True,
            )

        if issue_numbers_set is not None and int(issue_number or 0) not in issue_numbers_set:
            continue
        state = content.get('state')
        if state not in {'OPEN', 'CLOSED'}:
            continue

        body = content.get('body', '')
        if '<!--mute_list_start-->' not in body or '<!--mute_list_end-->' not in body:
            continue
        if updated_since_cutoff is not None:
            updated_dt = _parse_iso8601(content.get('updatedAt'))
            if updated_dt is None or updated_dt < updated_since_cutoff:
                continue
            stats['issues_recent_match'] += 1

        parsed = parse_body(body)
        tests_from_body, branches = parsed.tests, parsed.branches
        if branch not in branches:
            continue
        stats['issues_branch_match'] += 1

        issue_id = content.get('id')
        if not issue_id:
            continue

        existing_control_comments = _list_control_comments(issue_id)
        parsed_control_items = {}
        for comment in existing_control_comments:
            parsed_control_items.update(_parse_control_items(comment.get('body', '')))
        control_items = dict(parsed_control_items)

        # Canonical mode: once control comment exists and is parseable, use it as source of truth.
        has_canonical_control = bool(control_items)
        if has_canonical_control:
            stats['issues_control_source'] += 1
            tests = sorted(set(control_items.keys()))
            remaining_tests = [
                test_name
                for test_name in tests
                if (control_items.get(test_name) or {}).get('state') != 'resolved'
            ]
        else:
            stats['issues_legacy_source'] += 1
            tests = sorted(set(tests_from_body))
            previously_unmuted = _collect_issue_unmuted_tests(issue_id)
            remaining_tests = sorted(set(tests) - previously_unmuted)
            for test_name in tests:
                control_items.setdefault(
                    test_name,
                    {
                        'requested': False,
                        'state': 'active',
                        'status': 'idle',
                        'reason': '',
                        'requested_at': '',
                        'resolved_at': '',
                    },
                )

            if existing_control_comments and not parsed_control_items:
                print(
                    f"collect_manual_unmute_request_rows: warning issue #{issue_number} "
                    "has control comment markers but no parseable control rows, using legacy fallback"
                )

        remaining_tests = sorted(set(remaining_tests))
        if not remaining_tests:
            continue

        linked_prs = _extract_linked_development_pull_requests(content)
        has_linked_prs = bool(linked_prs)
        is_closed = state == 'CLOSED'
        close_actor_login, close_actor_type = _extract_close_actor(content)
        closed_by_human = is_closed and not _is_bot_actor(close_actor_login, close_actor_type)

        if is_closed:
            stats['issues_closed'] += 1
            if require_non_bot_close_actor and not closed_by_human:
                continue
            if closed_by_human:
                stats['issues_non_bot_closed'] += 1

        if has_linked_prs:
            stats['issues_with_linked_pr'] += 1

        auto_enable_manual_on_human_close = (
            sync_issue_comments
            and AUTO_ENABLE_MANUAL_ON_HUMAN_CLOSE
            and is_closed
            and closed_by_human
            and remaining_tests
            and (has_linked_prs or not require_linked_development_pr)
        )
        if (
            sync_issue_comments
            and AUTO_ENABLE_MANUAL_ON_HUMAN_CLOSE
            and is_closed
            and closed_by_human
            and remaining_tests
            and require_linked_development_pr
            and not has_linked_prs
        ):
            print(
                f"collect_manual_unmute_request_rows: issue #{issue_number} "
                "manual fast-unmute on human close skipped: no linked development PR"
            )

        if (
            auto_enable_manual_on_human_close
        ):
            reopened = reopen_issue(issue_id)
            if reopened:
                stats['issues_reopened_human_close'] += 1
                is_closed = False
                state = 'OPEN'

            if status_field_id and in_progress_option_id and project_item_id:
                update_issue_status(
                    issue_id=project_item_id,
                    status_field_id=status_field_id,
                    status_option_id=in_progress_option_id,
                    issue_url=content.get('url', ''),
                )
                stats['issues_set_in_progress'] += 1
            elif status_field_id and not in_progress_option_id:
                print(
                    f"collect_manual_unmute_request_rows: warning issue #{issue_number} "
                    "cannot set status In Progress (status option not found)"
                )

            comments = get_issue_comments(issue_id)
            if not _has_human_close_reopen_comment(comments):
                reason_comment = _build_human_close_reopen_comment(
                    issue_number=issue_number,
                    active_tests=remaining_tests,
                )
                add_issue_comment(issue_id, reason_comment)

        for test_name in tests:
            control_items.setdefault(
                test_name,
                {
                    'requested': False,
                    'state': 'active',
                    'status': 'idle',
                    'reason': '',
                    'requested_at': '',
                    'resolved_at': '',
                },
            )

        changed = False

        for test_name in remaining_tests:
            item = control_items[test_name]
            if item.get('state') == 'resolved':
                continue

            if auto_enable_manual_on_human_close and not item.get('requested'):
                item['requested'] = True
                changed = True

            if item.get('requested'):
                if not item.get('requested_at'):
                    item['requested_at'] = _isoformat_z(now)
                    changed = True
                normalized_status = normalize_manual_unmute_status(
                    item.get('status'),
                    requested=True,
                )
                if item.get('status') != normalized_status:
                    item['status'] = normalized_status
                    changed = True
                overrides.append(
                    {
                        'full_name': test_name,
                        'branch': branch,
                        'build_type': build_type,
                        'window_days': MANUAL_FAST_UNMUTE_WINDOW_DAYS,
                        'reason': 'manual_unmute_checkbox',
                        'issue_number': issue_number,
                        'requested_at': item.get('requested_at'),
                    }
                )
                stats['overrides_total'] += 1
            else:
                if item.get('status') != 'idle':
                    item['status'] = 'idle'
                    changed = True
                if item.get('requested_at'):
                    item['requested_at'] = ''
                    changed = True

        for test_name in tests:
            item = control_items[test_name]
            if item.get('state') == 'resolved':
                continue
            if test_name not in remaining_tests:
                item['state'] = 'resolved'
                item['status'] = 'resolved'
                item['reason'] = _derive_resolution_reason(
                    test_name,
                    had_manual_request=bool(item.get('requested')),
                    stable_unmute_candidates=stable_unmute_candidates,
                    delete_candidates=delete_candidates,
                )
                item['resolved_at'] = item.get('resolved_at') or _isoformat_z(now)
                item['requested'] = False
                item['requested_at'] = ''
                changed = True

        active_items = {name: control_items[name] for name in remaining_tests if control_items[name].get('state') != 'resolved'}
        active_chunks = _split_tests_into_chunks(active_items.keys())
        desired_parts = max(len(active_chunks), len(existing_control_comments), 1)
        rendered_parts = []
        for idx in range(desired_parts):
            part_tests = active_chunks[idx] if idx < len(active_chunks) else []
            part_items = {name: active_items[name] for name in part_tests}
            rendered_parts.append(
                _render_control_comment(
                    issue_number=issue_number,
                    part_idx=idx + 1,
                    part_total=desired_parts,
                    items=part_items,
                )
            )

        if sync_issue_comments:
            existing_sorted = sorted(existing_control_comments, key=lambda x: x.get('id', ''))
            for idx, rendered in enumerate(rendered_parts):
                if idx < len(existing_sorted):
                    old_body = existing_sorted[idx].get('body', '')
                    if old_body != rendered:
                        edit_issue_comment(existing_sorted[idx]['id'], rendered)
                else:
                    add_issue_comment(issue_id, rendered)

        if (
            sync_issue_comments
            and add_auto_comment
            and auto_enable_manual_on_human_close
            and has_linked_prs
        ):
            comments = get_issue_comments(issue_id)
            if not _has_fast_unmute_comment(comments):
                comment = _build_fast_unmute_comment(
                    issue_number=issue_number,
                    closer_login=close_actor_login or 'unknown',
                    linked_prs=linked_prs,
                )
                add_issue_comment(issue_id, comment)

        if sync_issue_comments and post_manual_rules_snapshot_comment:
            ydb_timelines = _load_daily_timeline_from_ydb(
                test_names=tests,
                branch=branch,
                build_type=build_type,
            )
            snapshot_comment = _build_manual_rules_snapshot_comment(
                issue_number=issue_number,
                branch=branch,
                build_type=build_type,
                tests=tests,
                control_items=control_items,
                ydb_timelines=ydb_timelines,
            )
            _upsert_marked_comment(
                issue_id=issue_id,
                marker=MANUAL_RULES_SNAPSHOT_COMMENT_MARKER,
                body=truncate_issue_body(snapshot_comment),
            )

        for test_name in tests:
            item = control_items[test_name]
            is_manual_active = bool(item.get('state') == 'active' and item.get('requested'))
            status = normalize_manual_unmute_status(
                item.get('status'),
                requested=is_manual_active,
            )

            effective_window = (
                MANUAL_FAST_UNMUTE_WINDOW_DAYS
                if is_manual_active
                else DEFAULT_UNMUTE_WINDOW_DAYS
            )

            request_rows.append(
                {
                    'issue_number': int(issue_number or 0),
                    'full_name': test_name,
                    'branch': branch,
                    'build_type': build_type,
                    'manual_unmute_status': status,
                    'manual_request_active': 1 if is_manual_active else 0,
                    'effective_unmute_window_days': int(effective_window),
                    'default_unmute_window_days': int(DEFAULT_UNMUTE_WINDOW_DAYS),
                    'manual_fast_unmute_window_days': int(MANUAL_FAST_UNMUTE_WINDOW_DAYS),
                    'resolution_reason': item.get('reason') or None,
                    'exported_at': now,
                }
            )

    print(
        "FAST_UNMUTE_OVERRIDE_COLLECT: "
        f"issues_total={stats['issues_total']}, "
        f"issues_recent_match={stats['issues_recent_match']}, "
        f"issues_closed={stats['issues_closed']}, "
        f"issues_non_bot_closed={stats['issues_non_bot_closed']}, "
        f"issues_with_linked_pr={stats['issues_with_linked_pr']}, "
        f"issues_branch_match={stats['issues_branch_match']}, "
        f"issues_control_source={stats['issues_control_source']}, "
        f"issues_legacy_source={stats['issues_legacy_source']}, "
        f"issues_reopened_human_close={stats['issues_reopened_human_close']}, "
        f"issues_set_in_progress={stats['issues_set_in_progress']}, "
        f"overrides_total={stats['overrides_total']}, "
        f"rows_total={len(request_rows)}"
    )
    return overrides, request_rows


def generate_github_issue_title_and_body(test_data):
    owner = test_data[0]['owner']
    branch = test_data[0]['branch']
    build_type = test_data[0].get('build_type') or DEFAULT_BUILD_TYPE
    test_full_names = [f"{d['full_name']}" for d in test_data]
    test_mute_strings = [f"{d['mute_string']}" for d in test_data]
    summary = [
        f"{d['test_name']}: {d['state']} last {d['days_in_state']} days, at {d['date_window']}: success_rate {d['success_rate']}%, {d['summary']}"
        for d in test_data
    ]

    # Преобразование списка тестов в строку и кодирование
    test_string = "\n".join(test_full_names)

    test_mute_strings_string = "\n".join(test_mute_strings)

    summary_string = "\n".join(summary)

    # Создаем ссылку на историю тестов, кодируя параметры

    test_name_params = "&".join(
        f"full_name={quote_plus(f'__in_{test}')}"
        for test in test_full_names
    )
    branch_param = f"&branch={branch}"
    test_run_history_link = f"{CURRENT_TEST_HISTORY_DASHBOARD}{test_name_params}{branch_param}"

    bt_suffix = f" [{build_type}]" if build_type != DEFAULT_BUILD_TYPE else ""
    # owner
    # Тело сообщения и кодирование
    body_template = (
        f"Mute:<!--mute_list_start-->\n"
        f"{test_string}\n"
        f"<!--mute_list_end-->\n\n"
        f"Branch:<!--branch_list_start-->\n"
        f"{branch}\n"
        f"<!--branch_list_end-->\n\n"
        f"Build type:<!--build_type_list_start-->\n"
        f"{build_type}\n"
        f"<!--build_type_list_end-->\n\n"
        f"**Add line to [muted_ya.txt](https://github.com/ydb-platform/ydb/blob/main/.github/config/muted_ya.txt):**\n"
        "```\n"
        f"{test_mute_strings_string}\n"
        "```\n\n"
        f"Owner: {owner}\n\n"
        f"**Fast unmute ({MANUAL_FAST_UNMUTE_WINDOW_DAYS}-day window):**\n"
        "- Bot maintains control comments with checkbox list for muted tests.\n"
        "- Mark `[x]` near a test to request manual fast-unmute for this test.\n"
        f"- `[x]` applies the **{MANUAL_FAST_UNMUTE_WINDOW_DAYS}-day** fast window immediately with configured pass/fail criteria.\n"
        "- If issue is closed manually (by human), all remaining active tests are auto-requested.\n\n"
        "**Read more in [mute_rules.md](https://github.com/ydb-platform/ydb/blob/main/.github/config/mute_rules.md)**\n\n"
        f"**Summary history:** \n {summary_string}\n"
        "\n\n"
        f"**Test run history:** [link]({test_run_history_link})\n\n"
        f"More info in [dashboard]({TEST_HISTORY_DASHBOARD})"
    )
    if len(test_full_names) > 1:
        title = f'Mute {test_data[0]["suite_folder"]} {len(test_full_names)} tests in {branch}{bt_suffix}'
    else:
        title = f'Mute {test_data[0]["full_name"]} in {branch}{bt_suffix}'

    return (
        title,
        body_template,
    )



def get_issues_and_tests_from_project(ORG_NAME, PROJECT_ID):
    issues = fetch_all_issues(ORG_NAME, PROJECT_ID)
    all_issues_with_contet = {}
    for issue in issues:
        content = issue['content']
        if content:
            body = content['body']
            parsed = parse_body(body)
            tests, branches = parsed.tests, parsed.branches

            field_values = issue.get('fieldValues', {}).get('nodes', [])
            for field_value in field_values:
                field_name = field_value.get('field', {}).get('name', '').lower()

                if field_name == "status" and 'name' in field_value:
                    status = field_value.get('name', 'N/A')
                    status_updated = field_value.get('updatedAt', '1970-01-0901T00:00:01Z')
                elif field_name == "owner" and 'name' in field_value:
                    owner = field_value.get('name', 'N/A')

            print(f"Issue ID: {content['id']}")
            print(f"Title: {content['title']}")
            print(f"URL: {content['url']}")
            print(f"State: {content['state']}")
            print(f"CreatedAt: {content['createdAt']}")
            print(f"Status: {status}")
            print(f"Status updated: {status_updated}")
            print(f"Owner: {owner}")
            print(f"Branch: {(',').join(branches) if branches else 'main'}")
            print("Tests:")

            all_issues_with_contet[content['id']] = {}
            all_issues_with_contet[content['id']]['title'] = content['title']
            all_issues_with_contet[content['id']]['url'] = content['url']
            all_issues_with_contet[content['id']]['state'] = content['state']
            all_issues_with_contet[content['id']]['createdAt'] = content['createdAt']
            all_issues_with_contet[content['id']]['status_updated'] = status_updated
            all_issues_with_contet[content['id']]['status'] = status
            all_issues_with_contet[content['id']]['owner'] = owner
            all_issues_with_contet[content['id']]['tests'] = []
            all_issues_with_contet[content['id']]['branches'] = branches
            all_issues_with_contet[content['id']]['build_type'] = parsed.build_type
            all_issues_with_contet[content['id']]['project_item_id'] = issue.get('id')

            for test in tests:
                all_issues_with_contet[content['id']]['tests'].append(test)
                print(f"- {test}")
            print('\n')

    return all_issues_with_contet


def get_muted_tests_from_issues():
    issues = get_issues_and_tests_from_project(ORG_NAME, PROJECT_ID)
    muted_tests = {}
    
    # First, collect all issues for each (test, build_type)
    for issue in issues:
        if issues[issue]["state"] != 'CLOSED':
            bt = issues[issue].get('build_type') or DEFAULT_BUILD_TYPE
            for test in issues[issue]['tests']:
                key = (test, bt)
                if key not in muted_tests:
                    muted_tests[key] = []
                muted_tests[key].append(
                    {
                        'url': issues[issue]['url'],
                        'createdAt': issues[issue]['createdAt'],
                        'status_updated': issues[issue]['status_updated'],
                        'status': issues[issue]['status'],
                        'state': issues[issue]['state'],
                        'branches': issues[issue]['branches'],
                        'build_type': bt,
                        'id': issue,
                        'project_item_id': issues[issue].get('project_item_id'),
                    }
                )
    
    # Then, for each (test, build_type), keep only the latest issue (by createdAt)
    for key in muted_tests:
        if len(muted_tests[key]) > 1:
            # Sort by createdAt (most recent first) and keep only the first one
            muted_tests[key] = sorted(
                muted_tests[key], 
                key=lambda x: x['createdAt'], 
                reverse=True
            )[:1]

    return muted_tests


def close_issue(issue_id):
    """Closes GitHub issue using GraphQL API.
    
    Args:
        issue_id (str): GitHub issue node ID
    """
    query = """
    mutation ($issueId: ID!) {
      closeIssue(input: {issueId: $issueId}) {
        issue {
          id
          url
        }
      }
    }
    """
    variables = {"issueId": issue_id}
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Issue {issue_id} closed")
    else:
        print(f"Error: Issue {issue_id} not closed")


def reopen_issue(issue_id):
    """Reopens GitHub issue using GraphQL API."""
    query = """
    mutation ($issueId: ID!) {
      reopenIssue(input: {issueId: $issueId}) {
        issue {
          id
          url
        }
      }
    }
    """
    variables = {"issueId": issue_id}
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Issue {issue_id} reopened")
        return True
    print(f"Error: Issue {issue_id} not reopened")
    return False


def add_issue_comment(issue_id, comment):
    """Adds a comment to GitHub issue using GraphQL API.
    
    Args:
        issue_id (str): GitHub issue node ID
        comment (str): Comment text
    """
    query = """
    mutation ($issueId: ID!, $body: String!) {
      addComment(input: {subjectId: $issueId, body: $body}) {
        commentEdge {
          node {
            id
          }
        }
      }
    }
    """
    variables = {"issueId": issue_id, "body": comment}
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Added comment to issue {issue_id}")
    else:
        print(f"Error: Failed to add comment to issue {issue_id}")

def update_issue_status(issue_id, status_field_id, status_option_id, issue_url):
    """Updates the status of an issue in the project.
    
    Args:
        issue_id (str): The ID of the issue
        status_field_id (str): The ID of the status field
        status_option_id (str): The ID of the status option to set
    """
    # Get project's global ID first
    query = """
    {
      organization(login: "%s") {
        projectV2(number: %s) {
          id
        }
      }
    }
    """ % (ORG_NAME, PROJECT_ID)
    
    result = run_query(query)
    if not result.get('data'):
        print("Error: Failed to fetch project ID")
        return
        
    project_global_id = result['data']['organization']['projectV2']['id']
    
    # Now update the status
    query = """
    mutation ($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId,
        itemId: $itemId,
        fieldId: $fieldId,
        value: {
          singleSelectOptionId: $optionId
        }
      }) {
        projectV2Item {
          id
        }
      }
    }
    """
    variables = {
        "projectId": project_global_id,
        "itemId": issue_id,
        "fieldId": status_field_id,
        "optionId": status_option_id
    }
    result = run_query(query, variables)
    if not result.get('errors'):
        print(f"Updated status for issue {issue_url}")
    else:
        print(f"Error: Failed to update status for issue {issue_url}")


def get_project_status_option_ids():
    """Return status field id and common option ids by lowercase name."""
    _, project_fields = get_project_v2_fields(ORG_NAME, PROJECT_ID)
    status_field_id = None
    option_ids = {}
    for field in project_fields:
        if not field.get('name'):
            continue
        if field['name'].lower() != "status":
            continue
        status_field_id = field['id']
        for option in field.get('options', []):
            option_name = (option.get('name') or '').strip().lower()
            if option_name:
                option_ids[option_name] = option.get('id')
        break
    return status_field_id, option_ids

def update_all_closed_issues_status(status_field_id, unmuted_option_id, target_issue_item_ids=None):
    """Updates status to Unmuted for selected closed issues in the project.
    
    Args:
        status_field_id (str): The ID of the status field
        unmuted_option_id (str): The ID of the Unmuted status option
    """
    target_set = set(target_issue_item_ids or [])
    has_next_page = True
    end_cursor = "null"
    
    while has_next_page:
        query = """
        {
          organization(login: "%s") {
            projectV2(number: %s) {
              items(first: 100, after: %s) {
                nodes {
                  id
                  content {
                    ... on Issue {
                      id
                      state
                      url
                    }
                  }
                  fieldValues(first: 20) {
                    nodes {
                      ... on ProjectV2ItemFieldSingleSelectValue {
                        field {
                          ... on ProjectV2SingleSelectField {
                            name
                          }
                        }
                        name
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
        """ % (ORG_NAME, PROJECT_ID, end_cursor)
        
        result = run_query(query)
        if not result.get('data'):
            print("Error: Failed to fetch project items")
            return
            
        items = result['data']['organization']['projectV2']['items']['nodes']
        for item in items:
            if item['content'] and item['content']['state'] == 'CLOSED':
                if target_set and item['id'] not in target_set:
                    continue
                # Check if status is not already Unmuted
                current_status = None
                for field_value in item['fieldValues']['nodes']:
                    if (field_value.get('field', {}).get('name', '').lower() == 'status' and 
                        'name' in field_value):
                        current_status = field_value.get('name')
                        break
                
                if current_status != 'Unmuted':
                    update_issue_status(item['id'], status_field_id, unmuted_option_id, item['content']['url'])
        
        # Update pagination info
        page_info = result['data']['organization']['projectV2']['items']['pageInfo']
        has_next_page = page_info['hasNextPage']
        end_cursor = f"\"{page_info['endCursor']}\"" if page_info['endCursor'] else "null"

def get_issue_comments(issue_id):
    """Gets all comments for an issue.
    
    Args:
        issue_id (str): The ID of the issue
        
    Returns:
        list: List of comment bodies
    """
    nodes = _fetch_issue_comment_nodes(issue_id)
    return [comment.get('body', '') for comment in nodes if comment.get('body') is not None]

def has_unmute_comment(comments, unmuted_tests):
    """Checks if there's already a comment about unmuting these tests.
    
    Args:
        comments (list): List of comment bodies
        unmuted_tests (list): List of unmuted test names
        
    Returns:
        bool: True if a comment about unmuting these tests exists
    """
    test_set = set(unmuted_tests)
    for comment in comments:
        if "tests have been unmuted" in comment:
            comment_tests = _extract_unmuted_tests_from_comment(comment)
            # If all current unmuted tests are in the comment, we don't need a new one
            if test_set.issubset(comment_tests):
                return True
    return False

def close_unmuted_issues(muted_tests_set, do_not_close_issues=False):
    """Closes issues where all tests are no longer muted.
    
    Args:
        muted_tests_set (set): Set of currently muted test names
        do_not_close_issues (bool): If True, issues will NOT be closed. If False, issues will be closed.
        
    Returns:
        tuple: (closed_issues, partially_unmuted_issues) - lists of dictionaries containing information about closed and partially unmuted issues
    """
    issues = get_muted_tests_from_issues()
    closed_issues = []
    partially_unmuted_issues = []
    closed_issue_ids = set()
    
    # Get status field ID and Unmuted option ID
    _, project_fields = get_project_v2_fields(ORG_NAME, PROJECT_ID)
    status_field_id = None
    unmuted_option_id = None
    for field in project_fields:
        if field.get('name') and field['name'].lower() == "status":
            status_field_id = field['id']
            for option in field['options']:
                if option['name'].lower() == "unmuted":
                    unmuted_option_id = option['id']
                    break
            break
    
    if not status_field_id or not unmuted_option_id:
        print("Warning: Could not find status field or Unmuted option")
        return closed_issues, partially_unmuted_issues
    
    # First, group tests by issue ID
    tests_by_issue = {}
    for key, issue_data_list in issues.items():
        test_name = key[0] if isinstance(key, tuple) else key
        for issue_data in issue_data_list:
            issue_id = issue_data['id']
            if issue_id not in tests_by_issue:
                tests_by_issue[issue_id] = {
                    'tests': set(),
                    'url': issue_data['url'],
                    'state': issue_data['state'],
                    'status': issue_data['status'],
                    'project_item_id': issue_data.get('project_item_id'),
                }
            tests_by_issue[issue_id]['tests'].add(test_name)
    
    # Then check each issue
    for issue_id, issue_info in tests_by_issue.items():
        if issue_info['state'] != 'CLOSED':
            unmuted_tests = [test for test in issue_info['tests'] if test not in muted_tests_set]
            if unmuted_tests:
                existing_comments = get_issue_comments(issue_id)
                if len(unmuted_tests) == len(issue_info['tests']):
                    # Полностью размьючен
                    if not has_unmute_comment(existing_comments, unmuted_tests):
                        comment = _build_unmute_comment("All tests have been unmuted:", unmuted_tests)
                        add_issue_comment(issue_id, comment)
                        if not do_not_close_issues:
                            close_issue(issue_id)
                        closed_issues.append({
                            'id': issue_id,
                            'url': issue_info['url'],
                            'tests': sorted(list(issue_info['tests']))
                        })
                        project_item_id = issue_info.get('project_item_id')
                        if project_item_id:
                            closed_issue_ids.add(project_item_id)
                        print(f"{'Would close' if do_not_close_issues else 'Closed'} issue as all its tests are no longer muted: {issue_info['url']}")
                        print(f"Unmuted tests: {', '.join(sorted(unmuted_tests))}")
                    # Если комментарий уже был — ничего не добавлять!
                elif 0 < len(unmuted_tests) < len(issue_info['tests']):
                    # Частично размьючен
                    if not has_unmute_comment(existing_comments, unmuted_tests):
                        comment = _build_unmute_comment("Some tests have been unmuted:", unmuted_tests)
                        add_issue_comment(issue_id, comment)
                        print(f"Added comment about unmuted tests to issue: {issue_info['url']}")
                        still_muted_tests = [test for test in issue_info['tests'] if test in muted_tests_set]
                        partially_unmuted_issues.append({
                            'url': issue_info['url'],
                            'unmuted_tests': sorted(unmuted_tests),
                            'still_muted_tests': sorted(still_muted_tests)
                        })
                    # Если комментарий уже был — ничего не добавлять!
    
    # Update status only for issues closed by this automation run.
    if closed_issue_ids:
        print(f"Updating status to Unmuted for closed issues: {len(closed_issue_ids)}")
        update_all_closed_issues_status(
            status_field_id,
            unmuted_option_id,
            target_issue_item_ids=closed_issue_ids,
        )
    
    return closed_issues, partially_unmuted_issues


def main():
    if "GITHUB_TOKEN" not in os.environ and "GH_TOKEN" not in os.environ:
        print("Error: Env variable GITHUB_TOKEN or GH_TOKEN is missing, skipping")
        return 1

    parser = argparse.ArgumentParser(description="GitHub mute issue utilities")
    subparsers = parser.add_subparsers(dest="command")

    sync_parser = subparsers.add_parser(
        "sync-manual-comments",
        help="Sync manual unmute control comments in issue list",
    )
    sync_parser.add_argument("--branch", default="main")
    sync_parser.add_argument("--build-type", dest="build_type", default="relwithdebinfo")
    sync_parser.add_argument(
        "--updated-since-hours",
        dest="updated_since_hours",
        type=int,
        default=24,
        help="Limit scan to issues updated in last N hours (0 disables filter)",
    )
    sync_parser.add_argument(
        "--issue-number",
        dest="issue_numbers",
        action="append",
        type=int,
        help="Process only specific issue number (can be passed multiple times)",
    )
    sync_parser.add_argument(
        "--post-manual-rules-snapshot-comment",
        action="store_true",
        help=(
            "Post an extra comment with per-test rule, ascii daily stability sketch, "
            "and test-history link"
        ),
    )
    sync_parser.add_argument(
        "--require-linked-development-pr",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Require linked development PR for auto-enabling manual fast-unmute "
            "on human-closed issues (default: false)"
        ),
    )

    args = parser.parse_args()

    if args.command == "sync-manual-comments":
        print(
            "Syncing manual unmute comments: "
            f"branch={args.branch}, build_type={args.build_type}, "
            f"updated_since_hours={args.updated_since_hours}, "
            f"issue_numbers={args.issue_numbers or 'all'}, "
            f"post_manual_rules_snapshot_comment={args.post_manual_rules_snapshot_comment}, "
            f"require_linked_development_pr={args.require_linked_development_pr}"
        )
        overrides, rows = collect_manual_unmute_request_rows(
            branch=args.branch,
            build_type=args.build_type,
            stable_unmute_candidates=set(),
            delete_candidates=set(),
            updated_since_hours=args.updated_since_hours,
            sync_issue_comments=True,
            issue_numbers=args.issue_numbers,
            post_manual_rules_snapshot_comment=args.post_manual_rules_snapshot_comment,
            require_linked_development_pr=args.require_linked_development_pr,
        )
        print(f"Done: overrides={len(overrides)}, rows={len(rows)}")
        return 0

    print("No command selected; nothing to do")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())