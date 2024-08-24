#!/usr/bin/env python
import datetime
import os
import json
import argparse
from github import Github, Auth as GithubAuth
from github.PullRequest import PullRequest


def update_pr_comment_text(pr: PullRequest, build_preset: str, run_number: int, color: str, text: str, rewrite: bool):
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
    parser.add_argument("text", type=argparse.FileType("r"), nargs="?", default="-")

    args = parser.parse_args()
    color = args.color

    run_number = int(os.environ.get("GITHUB_RUN_NUMBER"))
    build_preset = os.environ["BUILD_PRESET"]

    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))

    with open(os.environ["GITHUB_EVENT_PATH"]) as fp:
        event = json.load(fp)

    pr = gh.create_from_raw_data(PullRequest, event["pull_request"])
    text = args.text.read()
    if text.endswith("\n"):
        # dirty hack because echo adds a new line 
        # and 'echo | comment-pr.py' leads to an extra newline  
        text = text[:-1]

    update_pr_comment_text(pr, build_preset, run_number, color, text, args.rewrite)


if __name__ == "__main__":
    if os.environ.get('GITHUB_EVENT_NAME', '').startswith('pull_request'):
        main()
