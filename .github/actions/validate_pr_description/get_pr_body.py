#!/usr/bin/env python3
import os
import sys
import json
from github import Github, Auth as GithubAuth

def main():
    pr_number = os.getenv("PR_NUMBER", sys.argv[1] if len(sys.argv) > 1 else None)
    if not pr_number:
        print("::error::PR_NUMBER is required")
        sys.exit(1)
    
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        print("::error::GITHUB_TOKEN is not set")
        sys.exit(1)
    
    github_repo = os.getenv("GITHUB_REPOSITORY")
    if not github_repo:
        print("::error::GITHUB_REPOSITORY is not set")
        sys.exit(1)
    
    try:
        gh = Github(auth=GithubAuth.Token(github_token))
        repo = gh.get_repo(github_repo)
        pr = repo.get_pull(int(pr_number))
        
        # Save PR body for next step
        pr_body = pr.body or ""
        github_output = os.getenv("GITHUB_OUTPUT")
        if not github_output:
            print("::error::GITHUB_OUTPUT is not set")
            sys.exit(1)
        with open(github_output, "a") as f:
            # Use JSON encoding to safely handle multiline content and special characters
            f.write(f"pr_body={json.dumps(pr_body)}\n")
        
        # Update GITHUB_EVENT_PATH for post_status_to_github.py
        event_path = os.getenv("GITHUB_EVENT_PATH")
        if event_path:
            with open(event_path, "w") as f:
                json.dump({"pull_request": {
                    "number": pr.number,
                    "user": {"login": pr.user.login},
                    "head": {"ref": pr.head.ref, "sha": pr.head.sha},
                    "base": {"ref": pr.base.ref, "sha": pr.base.sha}
                }}, f)
    except Exception as e:
        print(f"::error::Failed to get PR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
