# .github/scripts/create_or_update_pr.py
import os
from github import Github

def create_or_update_pr():
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    BASE_BRANCH = os.getenv('BASE_BRANCH')
    BRANCH_FOR_PR = os.getenv('BRANCH_FOR_PR')
    TITLE = os.getenv('TITLE')
    BODY = os.getenv('BODY')

    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(os.getenv('GITHUB_REPOSITORY'))

    # Check for an existing PR
    existing_prs = repo.get_pulls(head=BRANCH_FOR_PR, base=BASE_BRANCH, state='open')
    existing_pr = None
    for pr in existing_prs:
        if pr.title == TITLE:
            existing_pr = pr
            break

    if existing_pr:
        print(f"Existing PR found. Updating PR #{existing_pr.number}.")
        # Update existing PR
        existing_pr.edit(title=TITLE, body=BODY)
    else:
        print("No existing PR found. Creating a new PR.")
        # Create new PR
        repo.create_pull(title=TITLE, body=BODY, head=BRANCH_FOR_PR, base=BASE_BRANCH)

if __name__ == '__main__':
    create_or_update_pr()