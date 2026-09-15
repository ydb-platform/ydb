#!/usr/bin/env python
import datetime
import os
import json
import argparse
from github import Github, Auth as GithubAuth
from github.PullRequest import PullRequest


def update_pr_comment_text(pr: PullRequest, build_preset: str, run_number: int, color: str, text: str, rewrite: bool, no_timestamp: bool = False):
    header = f"<!-- status pr={pr.number}, preset={build_preset}, run={run_number} -->"

    body = comment = None
    for c in pr.get_issue_comments():
        if c.body.startswith(header):
            print(f"found comment id={c.id}")
            comment = c
            if not rewrite:
                body = [c.body]
            break

    if body is None:
        body = [header]

    if no_timestamp:
        body.append(text)
    else:
        indicator = f":{color}_circle:"
        timestamp_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        body.append(f"{indicator} `{timestamp_str}` {text}")

    body = "\n".join(body)

    if comment is None:
        print(f"post new comment")
        pr.create_issue_comment(body)
    else:
        print(f"edit comment")
        comment.edit(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rewrite", dest="rewrite", action="store_true")
    parser.add_argument("--color", dest="color", default="white")
    parser.add_argument("--no-timestamp", dest="no_timestamp", action="store_true", help="Skip adding timestamp to comment")
    parser.add_argument("text", type=argparse.FileType("r"), nargs="?", default="-")

    args = parser.parse_args()
    color = args.color

    run_number = int(os.environ.get("GITHUB_RUN_NUMBER", os.environ.get("GITHUB_RUN_ID", "0")))
    build_preset = os.environ["BUILD_PRESET"]

    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))
    
    # Try to get PR from event or from PR_NUMBER env var
    pr = None
    event_name = os.environ.get('GITHUB_EVENT_NAME', '')
    
    if event_name.startswith('pull_request'):
        # Standard pull_request event
        with open(os.environ["GITHUB_EVENT_PATH"]) as fp:
            event = json.load(fp)
        pr = gh.create_from_raw_data(PullRequest, event["pull_request"])
    else:
        # workflow_call or workflow_dispatch - try to get PR_NUMBER from env
        # PR_NUMBER is just the number, e.g. "12345"
        pr_number = os.environ.get("PR_NUMBER")
        if pr_number:
            try:
                repo = gh.get_repo(os.environ["GITHUB_REPOSITORY"])
                pr = repo.get_pull(int(pr_number))
            except Exception as e:
                print(f"::warning::Failed to get PR {pr_number}: {e}")
                return
    
    if pr is None:
        print("::warning::No PR found, skipping comment")
        return

    text = args.text.read()
    if text.endswith("\n"):
        # dirty hack because echo adds a new line 
        # and 'echo | comment-pr.py' leads to an extra newline  
        text = text[:-1]

    update_pr_comment_text(pr, build_preset, run_number, color, text, args.rewrite, args.no_timestamp)


if __name__ == "__main__":
    main()
