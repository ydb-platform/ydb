#!/usr/bin/env python
import os
import json
import argparse
from github import Github, Auth as GithubAuth
from github.PullRequest import PullRequest
from gh_status import update_pr_comment_text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rewrite", dest="rewrite", action="store_true")
    parser.add_argument("--color", dest="color", default="white")
    parser.add_argument("--fail", dest="fail", action="store_true")
    parser.add_argument("--ok", dest="ok", action="store_true")
    parser.add_argument("text", type=argparse.FileType("r"), nargs="?", default="-")

    args = parser.parse_args()
    color = args.color

    if args.ok:
        color = 'green'
    elif args.fail:
        color = 'red'

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
