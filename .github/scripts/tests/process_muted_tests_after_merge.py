#!/usr/bin/env python3
"""
Helper script for processing muted tests after PR merge:
- Get muted_ya.txt from merge commit
- Comment PR
"""
import os
import sys
import argparse
import subprocess
from github import Github


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
    parser = argparse.ArgumentParser(description="Helper script for processing muted tests after PR merge")
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # get-muted-file command
    get_file_parser = subparsers.add_parser('get-muted-file', help='Get muted_ya.txt from merge commit')
    get_file_parser.add_argument('--merge_commit_sha', help='Merge commit SHA')
    get_file_parser.add_argument('--base_branch', required=True, help='Base branch')
    get_file_parser.add_argument('--output', default='muted_ya.txt', help='Output file path')
    
    # comment-pr command
    comment_parser = subparsers.add_parser('comment-pr', help='Comment on PR')
    comment_parser.add_argument('--pr_number', required=True, help='PR number')
    comment_parser.add_argument('--base_branch', required=True, help='Base branch')
    comment_parser.add_argument('--issues_file', help='Path to issues file (optional)')
    
    args = parser.parse_args()
    
    if args.command == 'get-muted-file':
        get_muted_ya_from_commit(args.merge_commit_sha, args.base_branch, args.output)
        print(f"✓ File saved to {args.output}")
    
    elif args.command == 'comment-pr':
        github_token = os.environ.get('GITHUB_TOKEN')
        if not github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required")
        
        repository = os.environ.get('GITHUB_REPOSITORY')
        if not repository:
            raise ValueError("GITHUB_REPOSITORY environment variable is required")
        
        run_id = os.environ.get('GITHUB_RUN_ID', '')
        run_number = os.environ.get('GITHUB_RUN_NUMBER', '')
        
        comment_pr(
            github_token,
            repository,
            args.pr_number,
            args.base_branch,
            args.issues_file,
            run_id,
            run_number
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
