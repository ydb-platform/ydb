# .github/scripts/create_or_update_pr.py
import os
from github import Github

def create_or_update_pr():
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    BRANCH = os.getenv('BRANCH')
    TITLE = os.getenv('TITLE')
    BODY = os.getenv('BODY')

    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(os.getenv('GITHUB_REPOSITORY'))

    # Check for an existing PR
    existing_prs = repo.get_pulls(head=BRANCH, base='main', state='open')
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
        repo.create_pull(title=TITLE, body=BODY, head=BRANCH, base='main')

if __name__ == '__main__':
    create_or_update_pr()
