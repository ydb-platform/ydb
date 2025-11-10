#!/usr/bin/env python3
"""
Process muted tests after PR merge:
- Get PR data from GitHub API
- Get muted_ya.txt from merge commit
- Create issues (if main branch)
- Send Telegram notifications (if main branch)
- Comment PR
"""
import os
import sys
import argparse
import subprocess
from pathlib import Path
from github import Github

# Add scripts directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'telegram'))


def get_pr_data(github_token, repository, event_name, pr_number=None):
    """Get PR data from GitHub API"""
    g = Github(github_token)
    repo = g.get_repo(repository)
    
    if not pr_number:
        raise ValueError("PR number is required")
    
    pr = repo.get_pull(int(pr_number))
    
    return {
        'number': pr.number,
        'base_branch': pr.base.ref,
        'merge_commit_sha': pr.merge_commit_sha,
        'merged': pr.merged,
        'labels': [label.name for label in pr.labels]
    }


def get_muted_ya_from_commit(merge_commit_sha, base_branch, output_path='muted_ya.txt'):
    """Get muted_ya.txt from merge commit or base branch"""
    try:
        if merge_commit_sha:
            print(f"Getting muted_ya.txt from merge commit: {merge_commit_sha}")
            subprocess.run(
                ['git', 'fetch', 'origin', merge_commit_sha],
                capture_output=True,
                text=True,
                check=True
            )
            result = subprocess.run(
                ['git', 'show', f'{merge_commit_sha}:.github/config/muted_ya.txt'],
                capture_output=True,
                text=True,
                check=True
            )
            with open(output_path, 'w') as f:
                f.write(result.stdout)
            print(f"✓ Retrieved muted_ya.txt from merge commit")
            return output_path
        else:
            print(f"⚠️ Merge commit SHA not available, trying to get from base branch: {base_branch}")
            subprocess.run(
                ['git', 'fetch', 'origin', base_branch],
                capture_output=True,
                text=True,
                check=True
            )
            result = subprocess.run(
                ['git', 'show', f'origin/{base_branch}:.github/config/muted_ya.txt'],
                capture_output=True,
                text=True,
                check=True
            )
            with open(output_path, 'w') as f:
                f.write(result.stdout)
            print(f"✓ Retrieved muted_ya.txt from base branch")
            return output_path
    except subprocess.CalledProcessError as e:
        print(f"Error getting muted_ya.txt: {e}")
        if e.stderr:
            print(f"stderr: {e.stderr}")
        raise


def create_issues_for_muted_tests(muted_ya_path, branch):
    """Create issues for muted tests"""
    print(f"Creating issues for muted tests from {muted_ya_path}")
    script_path = os.path.join(
        os.path.dirname(__file__),
        'create_new_muted_ya.py'
    )
    
    result = subprocess.run(
        [
            sys.executable, script_path, 'create_issues',
            '--file_path', os.path.abspath(muted_ya_path),
            '--branch', branch
        ],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error creating issues: {result.stderr}")
        return None
    
    issues_file = os.path.join(os.getcwd(), 'created_issues.txt')
    if os.path.exists(issues_file):
        print(f"✓ Issues created, results in {issues_file}")
        return issues_file
    return None


def send_telegram_notifications(issues_file, telegram_bot_token, team_channels):
    """Send Telegram notifications"""
    if not issues_file or not os.path.exists(issues_file):
        print("No issues file, skipping Telegram notifications")
        return
    
    print("Sending Telegram notifications...")
    script_path = os.path.join(
        os.path.dirname(__file__),
        '..', 'telegram', 'parse_and_send_team_issues.py'
    )
    
    result = subprocess.run(
        [
            sys.executable, script_path,
            '--on-mute-change-update',
            '--file', issues_file,
            '--bot-token', telegram_bot_token,
            '--team-channels', team_channels,
            '--include-plots',
            '--delay', '2'
        ],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error sending Telegram notifications: {result.stderr}")
    else:
        print("✓ Telegram notifications sent")


def comment_pr(github_token, repository, pr_number, base_branch, issues_file, run_id, run_number):
    """Comment on PR"""
    g = Github(github_token)
    repo = g.get_repo(repository)
    pr = repo.get_issue(int(pr_number))
    
    workflow_url = f"https://github.com/{repository}/actions/runs/{run_id}"
    body = f"Workflow completed for branch {base_branch} in workflow [#{run_number}]({workflow_url})\n\n"
    
    if issues_file and os.path.exists(issues_file):
        with open(issues_file, 'r') as f:
            body += f.read()
    else:
        body += "Muted tests data updated in YDB. Issues creation and Telegram notifications are only performed for main branch."
    
    pr.create_comment(body)
    print(f"✓ Comment added to PR #{pr_number}")


def main():
    parser = argparse.ArgumentParser(description="Process muted tests after PR merge")
    parser.add_argument('--event_name', required=True, help='GitHub event name')
    parser.add_argument('--pr_number', help='PR number')
    parser.add_argument('--merge_commit_sha', help='Merge commit SHA')
    parser.add_argument('--base_branch', help='Base branch')
    parser.add_argument('--build_type', default='relwithdebinfo', help='Build type')
    
    args = parser.parse_args()
    
    # Get GitHub token and repository from environment
    github_token = os.environ.get('GITHUB_TOKEN')
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is required")
    
    repository = os.environ.get('GITHUB_REPOSITORY')
    if not repository:
        raise ValueError("GITHUB_REPOSITORY environment variable is required")
    
    run_id = os.environ.get('GITHUB_RUN_ID', '')
    run_number = os.environ.get('GITHUB_RUN_NUMBER', '')
    
    # Get PR data if pr_number is provided
    pr_data = None
    if args.pr_number:
        print("Getting PR data from GitHub...")
        pr_data = get_pr_data(
            github_token,
            repository,
            args.event_name,
            args.pr_number
        )
    
    # Use provided values or from PR data
    base_branch = args.base_branch or (pr_data['base_branch'] if pr_data else None)
    merge_commit_sha = args.merge_commit_sha or (pr_data['merge_commit_sha'] if pr_data else None)
    pr_number = args.pr_number or (pr_data['number'] if pr_data else None)
    
    if not base_branch:
        raise ValueError("Base branch is required (provide --base_branch or --pr_number)")
    if not pr_number:
        raise ValueError("PR number is required (provide --pr_number)")
    
    is_main = base_branch == 'main'
    
    print(f"PR #{pr_number}: base_branch={base_branch}, is_main={is_main}")
    
    # Get muted_ya.txt from merge commit
    muted_ya_path = get_muted_ya_from_commit(merge_commit_sha, base_branch)
    
    # Create issues and send Telegram (only for main)
    issues_file = None
    if is_main:
        issues_file = create_issues_for_muted_tests(muted_ya_path, base_branch)
        
        telegram_bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        team_channels = os.environ.get('TEAM_CHANNELS')
        if telegram_bot_token and team_channels:
            send_telegram_notifications(issues_file, telegram_bot_token, team_channels)
    
    # Comment PR (for all branches)
    comment_pr(
        github_token,
        repository,
        pr_number,
        base_branch,
        issues_file,
        run_id,
        run_number
    )
    
    print("✓ Process completed successfully")


if __name__ == "__main__":
    main()

