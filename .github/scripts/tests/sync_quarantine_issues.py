#!/usr/bin/env python3

import argparse
import datetime
import os
import sys
from typing import Dict, List, Set, Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "analytics"))
from ydb_wrapper import YDBWrapper  # type: ignore[import-not-found]

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from github_issue_utils import DEFAULT_BUILD_TYPE, parse_body

from update_mute_issues import PROJECT_ID, fetch_all_issues, run_query


REOPEN_COMMENT_MARKER = "<!--mute-coordinator-quarantine-reopen:v1-->"


def _fetch_quarantine_tests(branch: str, build_type: str) -> Set[str]:
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return set()
        table_path = ydb_wrapper.get_table_path("mute_coordinator_control_state")
        query = f"""
            SELECT full_name
            FROM `{table_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND active = 1
              AND policy_type IN ('quarantine_user_fixed', 'quarantine_manual_unmute')
        """
        rows = ydb_wrapper.execute_scan_query(
            query,
            query_name=f"sync_quarantine_issues_fetch_state_{branch}_{build_type}",
        )
        return {str(row["full_name"]) for row in rows if row.get("full_name")}


def _fetch_quarantine_rule_details(branch: str, build_type: str) -> Dict[str, Tuple[str, str]]:
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return {}
        table_path = ydb_wrapper.get_table_path("mute_coordinator_effective_rule")
        query = f"""
            SELECT
                full_name,
                effective_rule_type,
                rule_valid_until
            FROM `{table_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND effective_rule_type IN (
                  'quarantine_user_fixed',
                  'quarantine_manual_unmute',
                  'quarantine_new_test'
              )
        """
        rows = ydb_wrapper.execute_scan_query(
            query,
            query_name=f"sync_quarantine_issues_fetch_effective_rules_{branch}_{build_type}",
        )
        out: Dict[str, Tuple[str, str]] = {}
        for row in rows:
            full_name = row.get("full_name")
            if not full_name:
                continue
            rule_type = str(row.get("effective_rule_type") or "")
            valid_until = row.get("rule_valid_until")
            valid_until_str = ""
            if isinstance(valid_until, datetime.datetime):
                valid_until_str = valid_until.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            out[str(full_name)] = (rule_type, valid_until_str)
        return out


def _fetch_resolved_stable_tests(branch: str, build_type: str) -> Set[str]:
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return set()
        table_path = ydb_wrapper.get_table_path("mute_coordinator_control_state")
        query = f"""
            SELECT full_name
            FROM `{table_path}`
            WHERE branch = '{branch}'
              AND build_type = '{build_type}'
              AND active = 1
              AND lifecycle_state = 'unmuted'
              AND decision_reason = 'stable_window_passed'
        """
        rows = ydb_wrapper.execute_scan_query(
            query,
            query_name=f"sync_quarantine_issues_fetch_resolved_{branch}_{build_type}",
        )
        return {str(row["full_name"]) for row in rows if row.get("full_name")}


def _reopen_issue(issue_id: str) -> bool:
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
    return not result.get("errors")


def _add_issue_comment(issue_id: str, comment: str) -> bool:
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
    return not result.get("errors")


def _get_issue_comments(issue_id: str) -> List[str]:
    query = """
    {
      node(id: "%s") {
        ... on Issue {
          comments(first: 100) {
            nodes {
              body
            }
          }
        }
      }
    }
    """ % issue_id
    result = run_query(query)
    nodes = result.get("data", {}).get("node", {}).get("comments", {}).get("nodes", [])
    return [node.get("body", "") for node in nodes]


def _issue_has_reopen_comment(issue_id: str) -> bool:
    comments = _get_issue_comments(issue_id)
    return any(REOPEN_COMMENT_MARKER in (comment or "") for comment in comments)


def _build_reopen_comment(
    issue_number: int,
    branch: str,
    build_type: str,
    tests: List[str],
    rule_details: Dict[str, Tuple[str, str]],
) -> str:
    preview_lines = []
    for name in tests[:20]:
        rule_type, valid_until = rule_details.get(name, ("unknown", ""))
        until_suffix = f", until `{valid_until}`" if valid_until else ""
        preview_lines.append(f"- `{name}` ({rule_type}{until_suffix})")
    preview = "\n".join(preview_lines)
    extra = ""
    if len(tests) > 20:
        extra = f"\n- ... and {len(tests) - 20} more"
    active_rule_types = sorted({rule_details.get(name, ("unknown", ""))[0] for name in tests})
    rules_line = ", ".join(active_rule_types) if active_rule_types else "unknown"
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return (
        f"{REOPEN_COMMENT_MARKER}\n"
        f"Issue #{issue_number} is reopened by mute coordinator.\n\n"
        f"Reason: tests linked to this issue are in active quarantine state for `{branch}` / `{build_type}`.\n"
        f"Active rule types: `{rules_line}`.\n"
        f"Generated at: {now}\n\n"
        f"Active quarantine tests ({len(tests)}):\n"
        f"{preview}{extra}\n"
    )


def _extract_issue_tests(content: Dict, branch: str, build_type: str) -> List[str]:
    body = (content or {}).get("body") or ""
    if "<!--mute_list_start-->" not in body or "<!--mute_list_end-->" not in body:
        return []
    parsed = parse_body(body)
    issue_branches = parsed.branches or []
    issue_build_type = parsed.build_type or DEFAULT_BUILD_TYPE
    if issue_branches and branch not in issue_branches:
        return []
    if issue_build_type != build_type:
        return []
    return sorted(set(parsed.tests))


def sync_quarantine_issues(branch: str, build_type: str) -> int:
    quarantine_tests = _fetch_quarantine_tests(branch=branch, build_type=build_type)
    rule_details = _fetch_quarantine_rule_details(branch=branch, build_type=build_type)
    resolved_stable_tests = _fetch_resolved_stable_tests(branch=branch, build_type=build_type)
    if not quarantine_tests:
        print(f"No quarantine tests found for branch={branch}, build_type={build_type}")
        return 0

    issues = fetch_all_issues(project_id=PROJECT_ID)
    reopened = 0
    scanned = 0

    for item in issues:
        content = (item or {}).get("content") or {}
        if not content:
            continue
        if (content.get("state") or "").upper() != "CLOSED":
            continue
        issue_id = content.get("id")
        issue_number = content.get("number")
        if not issue_id or issue_number is None:
            continue

        issue_tests = _extract_issue_tests(content, branch=branch, build_type=build_type)
        if not issue_tests:
            continue

        overlap = sorted((set(issue_tests) & quarantine_tests) - resolved_stable_tests)
        if not overlap:
            continue

        scanned += 1
        if not _reopen_issue(issue_id):
            print(f"Failed to reopen issue #{issue_number}")
            continue

        if not _issue_has_reopen_comment(issue_id):
            comment = _build_reopen_comment(
                issue_number=int(issue_number),
                branch=branch,
                build_type=build_type,
                tests=overlap,
                rule_details=rule_details,
            )
            _add_issue_comment(issue_id, comment)

        reopened += 1
        print(f"Reopened issue #{issue_number} (matched tests={len(overlap)})")

    print(
        "Quarantine issue sync complete: "
        f"branch={branch}, build_type={build_type}, "
        f"candidate_closed_issues={scanned}, reopened={reopened}, "
        f"resolved_stable_filtered={len(resolved_stable_tests)}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Reopen closed mute issues for quarantine tests")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--build-type", dest="build_type", default=DEFAULT_BUILD_TYPE)
    args = parser.parse_args()

    if "GITHUB_TOKEN" not in os.environ and "GH_TOKEN" in os.environ:
        os.environ["GITHUB_TOKEN"] = os.environ["GH_TOKEN"]

    if "GITHUB_TOKEN" not in os.environ:
        print("Error: GITHUB_TOKEN is required")
        return 1

    return sync_quarantine_issues(branch=args.branch, build_type=args.build_type)


if __name__ == "__main__":
    raise SystemExit(main())
