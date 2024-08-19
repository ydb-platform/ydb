import datetime
import platform
from github.PullRequest import PullRequest


def get_timestamp():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def get_platform_name():
    return f'{platform.system().lower()}-{platform.machine()}'


def update_pr_comment_text(pr: PullRequest, build_preset: str, color: str, text: str, rewrite: bool):
    platform_name = get_platform_name()
    header = f"<!-- status pr={pr.number}, preset={platform_name}-{build_preset} -->"

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
    body.append(f"{indicator} `{get_timestamp()}` {text}")

    body = "\n".join(body)

    if '{platform_name}' in body:
        # input can contain '{platform_name}'
        body = body.replace('{platform_name}', platform_name)

    if comment is None:
        print(f"post new comment")
        pr.create_issue_comment(body)
    else:
        print(f"edit comment")
        comment.edit(body)
