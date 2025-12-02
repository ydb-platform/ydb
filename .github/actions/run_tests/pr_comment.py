#!/usr/bin/env python3
"""
Script to create and update PR comments for test runs.
"""
import os
import sys
from github import Github, Auth as GithubAuth

def get_pr_number():
    """Extract PR number from environment variable."""
    pr_number = os.environ.get("PR_NUMBER")
    if not pr_number:
        raise ValueError("PR_NUMBER environment variable is not set")
    
    # Remove pull/ prefix if present
    if pr_number.startswith("pull/"):
        pr_number = pr_number.replace("pull/", "")
    
    return int(pr_number)

def get_workflow_run_url():
    """Get workflow run URL for identification."""
    github_server = os.environ.get("GITHUB_SERVER_URL")
    if not github_server:
        raise ValueError("GITHUB_SERVER_URL environment variable is not set")
    
    github_repo = os.environ.get("GITHUB_REPOSITORY")
    if not github_repo:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set")
    
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not run_id:
        raise ValueError("GITHUB_RUN_ID environment variable is not set")
    
    return f"{github_server}/{github_repo}/actions/runs/{run_id}"

def create_or_update_comment(pr_number, message, workflow_run_url):
    """Create or update PR comment with test run information."""
    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is not set")
    
    github_repo = os.environ.get("GITHUB_REPOSITORY")
    if not github_repo:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set")
    
    gh = Github(auth=GithubAuth.Token(github_token))
    repo = gh.get_repo(github_repo)
    pr = repo.get_pull(pr_number)
    
    # Find existing comment by workflow run URL
    comment = None
    for c in pr.get_issue_comments():
        if workflow_run_url in c.body:
            comment = c
            break
    
    # Add workflow run link to message
    full_body = f"{message}\n\n[View workflow run]({workflow_run_url})"
    
    if comment:
        print(f"::notice::Updating existing comment id={comment.id}")
        try:
            comment.edit(full_body)
        except Exception as e:
            print(f"::error::Failed to update comment id={comment.id}: {e}", file=sys.stderr)
            raise
    else:
        print(f"::notice::Creating new comment")
        try:
            pr.create_issue_comment(full_body)
        except Exception as e:
            print(f"::error::Failed to create new comment: {e}", file=sys.stderr)
            raise

def format_start_message(build_preset, test_size, test_targets):
    """Format message for test run start."""
    parts = []
    parts.append("🧪 **Test Run Started**")
    parts.append("")
    
    info = []
    info.append(f"**Build Preset:** `{build_preset}`")
    info.append(f"**Test Size:** `{test_size}`")
    
    if test_targets and test_targets != "ydb/":
        info.append(f"**Test Targets:** `{test_targets}`")
    
    parts.append("\n".join(info))
    parts.append("")
    parts.append("⏳ Tests are running...")
    
    return "\n".join(parts)

def format_completion_message(build_preset, test_size, test_targets, summary_content, status):
    """Format message for test run completion."""
    parts = []
    
    # Status emoji
    if status == "success":
        parts.append("✅ **Test Run Completed Successfully**")
    elif status == "failure":
        parts.append("❌ **Test Run Failed**")
    elif status == "cancelled":
        parts.append("⚠️ **Test Run Cancelled**")
    else:
        parts.append("⚠️ **Test Run Completed**")
    
    parts.append("")
    
    info = []
    info.append(f"**Build Preset:** `{build_preset}`")
    info.append(f"**Test Size:** `{test_size}`")
    
    if test_targets and test_targets != "ydb/":
        info.append(f"**Test Targets:** `{test_targets}`")
    
    parts.append("\n".join(info))
    parts.append("")
    
    # Add summary content if available
    if summary_content and summary_content.strip():
        parts.append("**Test Results:**")
        parts.append("")
        parts.append(summary_content.strip())
    
    return "\n".join(parts)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("::error::Usage: pr_comment.py <start|complete>")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command not in ["start", "complete"]:
        print(f"::error::Unknown command: {command}. Must be 'start' or 'complete'")
        sys.exit(1)
    
    pr_number = get_pr_number()
    
    build_preset = os.environ.get("BUILD_PRESET")
    if not build_preset:
        raise ValueError("BUILD_PRESET environment variable is not set")
    
    test_size = os.environ.get("TEST_SIZE")
    if not test_size:
        raise ValueError("TEST_SIZE environment variable is not set")
    
    test_targets = os.environ.get("TEST_TARGETS", "ydb/")
    
    workflow_run_url = get_workflow_run_url()
    
    if command == "start":
        message = format_start_message(build_preset, test_size, test_targets)
        create_or_update_comment(pr_number, message, workflow_run_url)
    else:  # complete
        status = os.environ.get("TEST_STATUS")
        if not status:
            raise ValueError("TEST_STATUS environment variable is not set")
        
        # Read summary from summary_text.txt in workspace
        workspace = os.environ.get("GITHUB_WORKSPACE")
        if not workspace:
            raise ValueError("GITHUB_WORKSPACE environment variable is not set")
        summary_text_path = os.path.join(workspace, "summary_text.txt")
        
        summary_content = ""
        if os.path.exists(summary_text_path):
            with open(summary_text_path, 'r', encoding='utf-8') as f:
                summary_content = f.read()
            if summary_content.strip():
                print(f"::notice::Read {len(summary_content)} characters from {summary_text_path}")
            else:
                print(f"::warning::Summary file {summary_text_path} is empty")
        else:
            print(f"::warning::Summary file not found: {summary_text_path}")
        
        message = format_completion_message(
            build_preset, test_size, test_targets,
            summary_content, status
        )
        create_or_update_comment(pr_number, message, workflow_run_url)

