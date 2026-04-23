#!/usr/bin/env python3
"""
Script to add run tests table comment to PR.
Used by add_run_tests_table.yml workflow.
"""

import os
import json
import urllib.parse
from github import Github, Auth as GithubAuth


def normalize_app_domain(app_domain: str) -> str:
    """Normalize app domain - remove https:// prefix if present."""
    domain = app_domain.strip()
    if domain.startswith("https://"):
        domain = domain[8:]
    if domain.startswith("http://"):
        domain = domain[7:]
    return domain.rstrip('/')


def generate_run_tests_table(pr_number: int, app_domain: str) -> str:
    """Generate run tests table with buttons for all build presets and test sizes."""
    domain = normalize_app_domain(app_domain)
    base_url = f"https://{domain}/workflow/trigger"
    repo_env = os.environ.get("GITHUB_REPOSITORY")
    if not repo_env or "/" not in repo_env:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set or malformed (expected 'owner/repo')")
    owner, repo = repo_env.split("/", 1)
    workflow_id = "run_tests.yml"
    return_url = f"https://github.com/{owner}/{repo}/pull/{pr_number}"
    
    # Build presets to show in rows
    build_presets = ["relwithdebinfo", "release-asan", "release-msan", "release-tsan"]
    # Test sizes: first column is small&medium, second column is large
    test_size_columns = [
        ("small,medium", "small&medium"),
        ("large", "large")
    ]
    
    def create_button(build_preset: str, test_size: str, label: str) -> str:
        """Create a button for specific build preset and test size."""
        params = {
            "owner": owner,
            "repo": repo,
            "workflow_id": workflow_id,
            "ref": "main",
            "pull_number": str(pr_number),
            "test_targets": "ydb/",
            "test_type": "unittest,py3test,py2test,pytest",
            "test_size": test_size,
            "additional_ya_make_args": "",
            "build_preset": build_preset,
            "collect_coredumps": "false",
            "return_url": return_url
        }
        query_string = "&".join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items()])
        url_ui = f"{base_url}?{query_string}&ui=true"
        # Badge with play icon and label - format: badge/message-color
        # Encode only spaces, keep emoji as is - use two spaces like in backport
        badge_text = f"▶  {label}".replace(" ", "%20")
        return f"[![▶  {label}](https://img.shields.io/badge/{badge_text}-4caf50)]({url_ui})"
    
    # Generate table
    comment = "<!-- run-tests-table -->\n"
    comment += "<h3>Run Extra Tests</h3>\n\n"
    comment += "Run additional tests for this PR. You can customize:\n"
    comment += "- **Test Size**: small, medium, large (default: all)\n"
    comment += "- **Test Targets**: any directory path (default: `ydb/`)\n"
    comment += "- **Sanitizers**: ASAN, MSAN, TSAN\n"
    comment += "- **Coredumps**: enable for debugging (default: off)\n"
    comment += "- **Additional args**: custom ya make arguments\n\n"
    comment += "| Build Preset | small&medium | large |\n"
    comment += "|--------------|--------------|------|\n"
    
    for build_preset in build_presets:
        row = f"| {build_preset} | "
        # First column: small&medium
        row += create_button(build_preset, test_size_columns[0][0], test_size_columns[0][1])
        row += " | "
        # Second column: large
        row += create_button(build_preset, test_size_columns[1][0], test_size_columns[1][1])
        row += " |\n"
        comment += row
    
    return comment


def create_or_update_pr_comment(pr, app_domain: str) -> None:
    """Create or update run tests table comment on PR.
    
    Args:
        pr: GitHub PullRequest object
        app_domain: Application domain for workflow URLs
    """
    try:
        pr_number = pr.number
        
        run_tests_table = generate_run_tests_table(pr_number, app_domain)
        header = "<!-- run-tests-table -->"
        
        # Check if comment with run tests table already exists
        existing_comment = None
        for comment in pr.get_issue_comments():
            if comment.body.startswith(header):
                existing_comment = comment
                break
        
        if existing_comment:
            # Update existing comment
            existing_comment.edit(run_tests_table)
            print(f"::notice::Updated run tests table comment on PR #{pr_number}")
        else:
            # Create new comment
            pr.create_issue_comment(run_tests_table)
            print(f"::notice::Created run tests table comment on PR #{pr_number}")
    except Exception as e:
        print(f"::error::Failed to create/update comment on PR #{pr_number}: {e}")
        raise


def main():
    """Main function to add run tests table to PR."""
    # Get PR info - either from event or from workflow_dispatch input
    pr_number_from_input = os.environ.get("PR_NUMBER")
    github_token = os.environ.get("GITHUB_TOKEN")
    github_repo = os.environ.get("GITHUB_REPOSITORY")
    
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is not set")
    if not github_repo:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set")
    
    gh = Github(auth=GithubAuth.Token(github_token))
    repo = gh.get_repo(github_repo)
    
    if pr_number_from_input:
        # workflow_dispatch mode - get PR by number
        pr_number = int(pr_number_from_input)
        pr = repo.get_pull(pr_number)
        print(f"::notice::workflow_dispatch mode: Adding run tests table to PR #{pr_number}")
    else:
        # pull_request event mode - get PR from event
        event_path = os.environ.get("GITHUB_EVENT_PATH")
        if not event_path:
            raise ValueError("GITHUB_EVENT_PATH environment variable is not set")
        
        if not os.path.exists(event_path):
            raise FileNotFoundError(f"Event file not found: {event_path}")
        
        with open(event_path, 'r') as f:
            event = json.load(f)
        
        if "pull_request" not in event:
            raise ValueError("Event does not contain pull_request data")
        
        pr_number = event["pull_request"]["number"]
        pr = repo.get_pull(pr_number)
    
    app_domain = os.environ.get("APP_DOMAIN")
    if not app_domain:
        raise ValueError("APP_DOMAIN environment variable is not set")
    
    create_or_update_pr_comment(pr, app_domain)


if __name__ == "__main__":
    main()

