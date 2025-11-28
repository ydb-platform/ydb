import datetime
import os
from github import Github, GithubException, Auth as GithubAuth

def post(is_valid, error_description):
    gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))
    
    pr_number = os.environ.get("PR_NUMBER")
    if not pr_number:
        print("::warning::PR_NUMBER is not set, skipping status update")
        return
    
    try:
        github_repo = os.environ.get("GITHUB_REPOSITORY")
        if not github_repo:
            print("::warning::GITHUB_REPOSITORY is not set, skipping status update")
            return
        repo = gh.get_repo(github_repo)
        pr = repo.get_pull(int(pr_number))
    except (ValueError, GithubException) as e:
        print(f"::warning::Could not get PR #{pr_number}: {e}, skipping status update")
        return

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
