#!/usr/bin/env python3
import os
import argparse
from github import Github

def read_body_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def get_body_content(body_input):
    """Determines if the body content is a file path or direct text."""
    if os.path.isfile(body_input):
        print(f"Body content will be read from file: {body_input}.")
        return read_body_from_file(body_input)
    else:
        print(f"Body content will be taken directly: '{body_input}.'")
        return body_input

def create_or_update_pr(args, repo):
    current_pr = None
    pr_number = None
    body = get_body_content(args.body)

    # Check for an existing PR
    existing_prs = repo.get_pulls(head=args.branch_for_pr, base=args.base_branch, state='open')
    for pr in existing_prs:
        if pr.base.ref == args.base_branch and pr.head.ref == args.branch_for_pr:
            current_pr = pr
            break
    if current_pr:
        print(f"Existing PR found. Updating PR #{current_pr.number}.")
        current_pr.edit(title=args.title, body=body)
    else:
        print("No existing PR found. Creating a new PR.")
        current_pr = repo.create_pull(title=args.title, body=body, head=args.branch_for_pr, base=args.base_branch)

    pr_number = current_pr.number
    if os.environ['GITHUB_OUTPUT']:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as gh_out:
            print(f"pr_number={pr_number}", file=gh_out)

    print(f"PR created or updated successfully. PR number: {pr_number}")

def append_to_pr_body(args, repo):
    body_to_append = get_body_content(args.body)
    
    pr = None
    if args.pr_number:
        pr = repo.get_pull(args.pr_number)
    else:
        existing_prs = repo.get_pulls(head=args.branch_for_pr, base=args.base_branch, state='open')
        for p in existing_prs:
            if p.base.ref == args.base_branch and p.head.ref == args.branch_for_pr:
                pr = p
                break

    if pr:
        print(f"Appending to PR #{pr.number}.")
        current_body = pr.body or ""
        new_body = current_body + "\n\n" + body_to_append
        pr.edit(body=new_body)
    else:
        print("No matching pull request found to append body.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Operate on a GitHub Pull Request')
    subparsers = parser.add_subparsers(dest='mode', required=True, help='Mode of operation')

    # Subparser for create or update PR mode
    create_parser = subparsers.add_parser('create_or_update', help='Create or update a pull request')
    create_parser.add_argument('--base_branch', type=str, required=True, help='Base branch for the PR')
    create_parser.add_argument('--branch_for_pr', type=str, required=True, help='Branch from which to create the PR')
    create_parser.add_argument('--title', type=str, required=True, help='Title of the PR')
    create_parser.add_argument('--body', type=str, default='', required=False, help='Body content of the PR, or path to a file with the content')

    # Subparser for append PR body mode
    append_parser = subparsers.add_parser('append_pr_body', help='Append text to the body of an existing pull request')
    group = append_parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--pr_number', type=int, help='Pull request number')
    append_parser.add_argument('--body', type=str, required=True, help='Text to append to the PR body')

    args = parser.parse_args()

    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN environment variable is not set")

    g = Github(GITHUB_TOKEN)
    repo_name = os.getenv('GITHUB_REPOSITORY', 'ydb-platform/ydb')
    repo = g.get_repo(repo_name)

    if args.mode == "create_or_update":
        create_or_update_pr(args, repo)
    elif args.mode == "append_pr_body":
        append_to_pr_body(args, repo)
