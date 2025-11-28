#!/usr/bin/env python3
import os
import argparse
from github import Github


def read_body_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()


def create_gist_for_large_content(content, github_token, title="PR Body Content"):
    """Creates a GitHub gist for large content and returns the gist URL."""
    from github import Github
    from github.InputFileContent import InputFileContent
    
    g = Github(github_token)
    
    # Create gist with the full content
    gist = g.get_user().create_gist(
        public=False,
        files={
            f"{title}.md": InputFileContent(content)
        },
        description=f"Large content for {title}"
    )
    
    print(f"Created gist: {gist.html_url}")
    return gist.html_url


def get_body_content(body_input, github_token=None):
    """Determines if the body content is a file path or direct text."""
    if os.path.isfile(body_input):
        print(f"Body content will be read from file: {body_input}.")
        content = read_body_from_file(body_input)
    else:
        print(f"Body content will be taken directly: '{body_input}.'")
        content = body_input
    
    # GitHub has a 65,536 character limit for PR body, so we use half of it to leave some space for the summary and closed issues
    MAX_BODY_LENGTH = 65536 // 2    
    
    if len(content) > MAX_BODY_LENGTH:
        print(f"Warning: PR body content is {len(content)} characters, exceeding GitHub's limit of {MAX_BODY_LENGTH}")
        
        if github_token:
            print("Creating GitHub gist for large content...")
            gist_url = create_gist_for_large_content(content, github_token, "Muted Tests Update Details")
            
            # Create a summary body with link to gist
            summary_content = f"""# Muted tests update

This PR contains a large number of test changes. Full details are available in the [GitHub Gist]({gist_url}).

## Summary
- **Total content size**: {len(content):,} characters
- **Content type**: Muted tests update details
- **Full details**: [View complete details in Gist]({gist_url})

---
*This summary was automatically generated due to content size limitations.*"""
            
            print(f"Created summary body with gist link: {len(summary_content)} characters")
            return summary_content
        else:
            print("No GitHub token available for gist creation. Truncating content...")
            # Fallback to truncation if no token
            truncation_notice = "\n\n---\n**Note: Content truncated due to length limits. See workflow logs for full details.**"
            available_length = MAX_BODY_LENGTH - len(truncation_notice)
            content = content[:available_length] + truncation_notice
            print(f"Truncated content to {len(content)} characters")
    
    return content


def create_or_update_pr(args, repo):
    current_pr = None
    pr_number = None
    github_token = os.getenv('GITHUB_TOKEN')
    body = get_body_content(args.body, github_token)

    owner = repo.owner.login
    head_format = f"{owner}:{args.branch_for_pr}"

    print(f"Searching for PR with head branch '{head_format}' and base branch '{args.base_branch}'")

    existing_prs = repo.get_pulls(head=head_format, base=args.base_branch, state='open')

    if existing_prs.totalCount > 0:
        current_pr = existing_prs[0]
        print(f"Found existing PR #{current_pr.number}: {current_pr.title}")

    if current_pr:
        print(f"Updating existing PR #{current_pr.number}.")
        current_pr.edit(title=args.title, body=body)
        print(f"PR #{current_pr.number} updated successfully.")
    else:
        print(f"No existing PR found. Creating a new PR from '{args.branch_for_pr}' to '{args.base_branch}'.")
        current_pr = repo.create_pull(title=args.title, body=body, head=args.branch_for_pr, base=args.base_branch)
        print(f"New PR #{current_pr.number} created successfully.")

    pr_number = current_pr.number
    github_output = os.environ.get('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a') as gh_out:
            print(f"pr_number={pr_number}", file=gh_out)

    print(f"PR operation completed. PR number: {pr_number}")
    return pr_number


def append_to_pr_body(args, repo):
    github_token = os.getenv('GITHUB_TOKEN')
    body_to_append = get_body_content(args.body, github_token)

    print(f"Looking for PR by number: {args.pr_number}")
    pr = repo.get_pull(args.pr_number)

    if pr:
        print(f"Appending to PR #{pr.number}.")
        current_body = pr.body or ""
        new_body = current_body + "\n\n" + body_to_append
        pr.edit(body=new_body)
        print(f"PR #{pr.number} body updated successfully.")
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
