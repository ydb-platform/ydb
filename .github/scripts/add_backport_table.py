#!/usr/bin/env python3
"""
Script to add backport table comment to PR after merge.
Used by add_backport_table.yml workflow.
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


def generate_backport_table(pr_number: int, app_domain: str) -> str:
    """Generate backport execution table with buttons for different branches."""
    domain = normalize_app_domain(app_domain)
    base_url = f"https://{domain}/workflow/trigger"
    repo_env = os.environ.get("GITHUB_REPOSITORY")
    if not repo_env or "/" not in repo_env:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set or malformed (expected 'owner/repo')")
    owner, repo = repo_env.split("/", 1)
    workflow_id = "cherry_pick_v2.yml"
    return_url = f"https://github.com/{owner}/{repo}/pull/{pr_number}"
    
    # Load backport branches from config
    workspace = os.environ.get("GITHUB_WORKSPACE")
    if not workspace:
        raise ValueError("GITHUB_WORKSPACE environment variable is not set")
    
    backport_branches_path = os.path.join(workspace, ".github", "config", "backport_branches.json")
    
    if not os.path.exists(backport_branches_path):
        raise FileNotFoundError(f"Backport branches config file not found: {backport_branches_path}")
    
    with open(backport_branches_path, 'r') as f:
        branches = json.load(f)
    
    if not isinstance(branches, list) or len(branches) == 0:
        raise ValueError(f"Invalid backport branches config: expected non-empty list, got {type(branches)}")
    
    print(f"::notice::Loaded {len(branches)} backport branch entries from {backport_branches_path}")
    
    # Collect all unique branches from all entries for manual button
    all_unique_branches = set()
    for branch_entry in branches:
        # Split by comma and strip whitespace
        branch_list = [b.strip() for b in branch_entry.split(',')]
        all_unique_branches.update(branch_list)
    
    # Sort for consistent output
    all_unique_branches_sorted = sorted(all_unique_branches)
    all_branches = ",".join(all_unique_branches_sorted)
    
    # Use each entry as is - each entry is a comma-separated list of branches for one backport
    rows = []
    for branch_entry in branches:
        # Use the branch entry as is (may contain multiple branches separated by comma)
        branch_value = branch_entry.strip()
        
        # Format branches for display: split and join with ", " (comma with space)
        branch_list = [b.strip() for b in branch_value.split(',')]
        branch_display = ", ".join(branch_list)
        
        # Use PR number - workflow_dispatch input name is commits_and_prs
        params = {
            "owner": owner,
            "repo": repo,
            "workflow_id": workflow_id,
            "ref": "main",
            "commits_and_prs": str(pr_number),  # workflow_dispatch input parameter name
            "target_branches": branch_value,  # Use original value for URL parameter
            "allow_unmerged": "true",
            "return_url": return_url
        }
        query_string = "&".join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items()])
        url_ui = f"{base_url}?{query_string}&ui=true"
        
        # Badge with only message (no label) - format: badge/message-color
        # Encode only spaces, keep emoji as is
        badge_text = "▶  Backport".replace(" ", "%20")
        button = f"[![▶  Backport](https://img.shields.io/badge/{badge_text}-4caf50)]({url_ui})"
        rows.append(f"| `{branch_display}` | {button} |")
    
    # Generate URL for backporting all unique branches (manual button)
    params_manual = {
        "owner": owner,
        "repo": repo,
        "workflow_id": workflow_id,
        "ref": "main",
        "commits_and_prs": str(pr_number),
        "target_branches": all_branches,
        "allow_unmerged": "true",
        "return_url": return_url
    }
    query_string_manual = "&".join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params_manual.items()])
    url_manual_ui = f"{base_url}?{query_string_manual}&ui=true"
    
    # Badge with only message for manual button
    # Encode only spaces, keep emoji and parentheses as is (shields.io handles them)
    badge_text_manual = "▶  Backport manual".replace(" ", "%20")
    
    table = "<!-- backport-table -->\n"
    table += "<h3>Backport</h3>\n\n"
    table += "To backport this PR, click the button next to the target branch and then click \"Run workflow\" in the Run Actions UI.\n\n"
    table += "| Branch | Run |\n"
    table += "|--------|-----|\n"
    table += "\n".join(rows)
    table += "\n\n"
    table += f"[![▶  Backport manual](https://img.shields.io/badge/{badge_text_manual}-2196F3)]({url_manual_ui})"
    return table


def create_or_update_pr_comment(pr, app_domain: str) -> None:
    """Create or update backport table comment on PR.
    
    Args:
        pr: GitHub PullRequest object
        app_domain: Application domain for workflow URLs
    """
    try:
        pr_number = pr.number
        
        backport_table = generate_backport_table(pr_number, app_domain)
        header = "<!-- backport-table -->"
        
        # Check if comment with backport table already exists
        existing_comment = None
        for comment in pr.get_issue_comments():
            if comment.body.startswith(header):
                existing_comment = comment
                break
        
        if existing_comment:
            # Update existing comment
            existing_comment.edit(backport_table)
            print(f"::notice::Updated backport table comment on PR #{pr_number}")
        else:
            # Create new comment
            pr.create_issue_comment(backport_table)
            print(f"::notice::Created backport table comment on PR #{pr_number}")
    except Exception as e:
        print(f"::error::Failed to create/update comment on PR #{pr_number}: {e}")
        raise


def main():
    """Main function to add backport table to PR body."""
    # Check if feature is enabled (skip check for workflow_dispatch)
    pr_number_from_input = os.environ.get("PR_NUMBER")
    is_workflow_dispatch = bool(pr_number_from_input)
    
    if not is_workflow_dispatch:
        show_backport_table = os.environ.get("SHOW_BACKPORT_IN_PR", "").upper() == "TRUE"
        if not show_backport_table:
            print("::notice::SHOW_BACKPORT_IN_PR is not set to TRUE, skipping backport table addition")
            return
    
    # Get PR info - either from event or from workflow_dispatch input
    github_token = os.environ.get("GITHUB_TOKEN")
    github_repo = os.environ.get("GITHUB_REPOSITORY")
    
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is not set")
    if not github_repo:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set")
    
    gh = Github(auth=GithubAuth.Token(github_token))
    repo = gh.get_repo(github_repo)
    
    if pr_number_from_input:
        # workflow_dispatch mode - get PR by number (no checks for merged or base branch)
        pr_number = int(pr_number_from_input)
        pr = repo.get_pull(pr_number)
        print(f"::notice::workflow_dispatch mode: Adding backport table to PR #{pr_number} (skipping merged/base branch checks)")
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
        base_ref = event["pull_request"]["base"]["ref"]
        
        # Check if PR is merged into main
        if not event["pull_request"].get("merged"):
            print(f"::notice::PR #{pr_number} is not merged, skipping backport table addition")
            return
        
        # Check if PR is merged into main branch
        if base_ref != "main":
            print(f"::notice::PR #{pr_number} is merged into {base_ref}, not main. Skipping backport table addition")
            return
        
        # Get PR object for consistency
        pr = repo.get_pull(pr_number)
    
    app_domain = os.environ.get("APP_DOMAIN")
    if not app_domain:
        raise ValueError("APP_DOMAIN environment variable is not set (required when SHOW_BACKPORT_IN_PR=TRUE)")
    
    create_or_update_pr_comment(pr, app_domain)


if __name__ == "__main__":
    main()

