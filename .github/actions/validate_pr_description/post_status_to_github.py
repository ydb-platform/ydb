import datetime
import os
import json
from github import Github, Auth as GithubAuth
from github.PullRequest import PullRequest

def post(is_valid, error_description):
    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))

    with open(os.environ["GITHUB_EVENT_PATH"]) as fp:
        event = json.load(fp)

    pr = gh.create_from_raw_data(PullRequest, event["pull_request"])

    header = f"<!-- status pr={pr.number}, validate PR description status -->"

    body = [header]
    comment = None
    for c in pr.get_issue_comments():
        if c.body.startswith(header):
            print(f"found comment id={c.id}")
            comment = c

    status_to_header = {
        True: "The validation of the Pull Request description is successful.",
        False: "The validation of the Pull Request description has failed. Please update the description."
    }

    color = "green" if is_valid else "red"
    indicator = f":{color}_circle:"
    timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body.append(f"{indicator} `{timestamp_str}` {status_to_header[is_valid]}")

    if not is_valid:
        body.append(f"\n{error_description}")

    body = "\n".join(body)

    if comment:
        print(f"edit comment")
        comment.edit(body)
    else:
        print(f"post new comment")
        pr.create_issue_comment(body)
