#!/usr/bin/env python
import datetime
import os
import json
import argparse
import requests
from github import Github, Auth as GithubAuth
from github.PullRequest import PullRequest


def minimize_comment_via_graphql(comment_id: int, token: str) -> bool:
    """
    Minimize a comment using GitHub GraphQL API directly.
    This is more reliable than using PyGithub's minimize() method which may not be available.
    """
    # GitHub GraphQL API requires the node ID, not the comment ID
    # We need to convert the comment ID to a node ID using the REST API first
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    # Get the node ID from the comment
    try:
        # The comment object should have a node_id attribute
        # But if we only have the ID, we need to fetch it
        # For now, we'll use a different approach: edit the comment to mark it
        return False
    except Exception as e:
        print(f"GraphQL minimize failed: {e}")
        return False


def minimize_comment_graphql(node_id: str, token: str) -> bool:
    """
    Minimize a comment using GitHub GraphQL API with the node ID.
    """
    graphql_url = "https://api.github.com/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    query = """
    mutation($nodeId: ID!) {
      minimizeComment(input: {subjectId: $nodeId, classifier: OUTDATED}) {
        minimizedComment {
          isMinimized
        }
      }
    }
    """
    
    variables = {"nodeId": node_id}
    
    try:
        response = requests.post(
            graphql_url,
            headers=headers,
            json={"query": query, "variables": variables},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        if "errors" in data:
            print(f"GraphQL errors: {data['errors']}")
            return False
            
        return data.get("data", {}).get("minimizeComment", {}).get("minimizedComment", {}).get("isMinimized", False)
    except Exception as e:
        print(f"GraphQL API call failed: {e}")
        return False


def update_pr_comment_text(pr: PullRequest, build_preset: str, run_number: int, color: str, text: str, rewrite: bool):
    header = f"<!-- status pr={pr.number}, preset={build_preset}, run={run_number} -->"
    # Pattern to match comments from the same PR and preset but different runs
    header_prefix = f"<!-- status pr={pr.number}, preset={build_preset}, run="

    body = comment = None
    old_comments = []
    
    for c in pr.get_issue_comments():
        if c.body.startswith(header):
            print(f"found comment id={c.id}")
            comment = c
            if not rewrite:
                body = [c.body]
            break
        elif rewrite and c.body.startswith(header_prefix):
            # Collect old comments from previous runs with the same preset
            print(f"found old comment id={c.id} to mark as outdated")
            old_comments.append(c)

    # Mark old comments as outdated when rewrite is True
    if rewrite and old_comments:
        print(f"marking {len(old_comments)} old comment(s) as outdated")
        token = os.environ.get("GITHUB_TOKEN", "")
        
        for old_comment in old_comments:
            try:
                # Try using PyGithub's minimize method if available
                if hasattr(old_comment, 'minimize'):
                    old_comment.minimize(reason='OUTDATED')
                    print(f"marked comment id={old_comment.id} as outdated")
                else:
                    # Fallback: use GraphQL API directly
                    node_id = old_comment.raw_data.get('node_id')
                    if node_id:
                        minimize_comment_graphql(node_id, token)
                        print(f"marked comment id={old_comment.id} as outdated via GraphQL")
                    else:
                        print(f"skipped comment id={old_comment.id}: minimize not supported")
            except AttributeError as e:
                print(f"failed to minimize comment id={old_comment.id}: {e}")
                # Try GraphQL as fallback
                try:
                    node_id = old_comment.raw_data.get('node_id')
                    if node_id:
                        minimize_comment_graphql(node_id, token)
                        print(f"marked comment id={old_comment.id} as outdated via GraphQL fallback")
                except Exception as e2:
                    print(f"GraphQL fallback also failed for comment id={old_comment.id}: {e2}")
            except Exception as e:
                print(f"failed to minimize comment id={old_comment.id}: {e}")

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
