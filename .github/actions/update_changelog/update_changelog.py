import functools
import sys
import json
import re
import subprocess
import requests

import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../validate_pr_description")))
from validate_pr_description import validate_pr_description

UNRELEASED = "Unreleased"
UNCATEGORIZED = "Uncategorized"
VERSION_PREFIX = "## "
CATEGORY_PREFIX = "### "
ITEM_PREFIX = "* "

GH_TOKEN = os.getenv("GH_TOKEN")

@functools.cache
def get_github_api_url():
   return os.getenv('GITHUB_REPOSITORY')

def to_dict(changelog_path, encoding='utf-8'):
    changelog = {}
    current_version = UNRELEASED
    current_category = UNCATEGORIZED
    pr_number = None
    changelog[current_version] = {}
    changelog[current_version][current_category] = {}
    
    if not os.path.exists(changelog_path):
        return changelog

    with open(changelog_path, 'r', encoding=encoding) as file:
        for line in file:
            if line.startswith(VERSION_PREFIX):
                current_version = line.strip().strip(VERSION_PREFIX)
                pr_number = None
                changelog[current_version] = {}
            elif line.startswith(CATEGORY_PREFIX):
                current_category = line.strip().strip(CATEGORY_PREFIX)
                pr_number = None
                changelog[current_version][current_category] = {}
            elif line.startswith(ITEM_PREFIX):
                pr_number = extract_pr_number(line)
                changelog[current_version][current_category][pr_number] = line.strip(f"{ITEM_PREFIX}{pr_number}:")
            elif pr_number:
                changelog[current_version][current_category][pr_number] += f"{line}"
    
    return changelog

def to_file(changelog_path, changelog):
    with open(changelog_path, 'w', encoding='utf-8') as file:
        if UNRELEASED in changelog:
            file.write(f"{VERSION_PREFIX}{UNRELEASED}\n\n")
            for category, items in changelog[UNRELEASED].items():
                if(len(changelog[UNRELEASED][category]) == 0):
                    continue
                file.write(f"{CATEGORY_PREFIX}{category}\n")
                for id, body in items.items():
                    file.write(f"{ITEM_PREFIX}{id}:{body.strip()}\n")
                file.write("\n")

        for version, categories in changelog.items():
            if version == UNRELEASED:
                continue
            file.write(f"{VERSION_PREFIX}{version}\n\n")
            for category, items in categories.items():
                if(len(changelog[version][category]) == 0):
                    continue
                file.write(f"{CATEGORY_PREFIX}{category}\n")
                for id, body in items.items():
                    file.write(f"{ITEM_PREFIX}{id}:{body.strip()}\n")
                file.write("\n")

def extract_changelog_category(description):
    category_section = re.search(r"### Changelog category.*?\n(.*?)(\n###|$)", description, re.DOTALL)
    if category_section:
        categories = [line.strip('* ').strip() for line in category_section.group(1).splitlines() if line.strip()]
        if len(categories) == 1:
            return categories[0]
    return None

def extract_pr_number(changelog_entry):
    match = re.search(r"#(\d+)", changelog_entry)
    if match:
        return int(match.group(1))
    return None

def extract_changelog_body(description):
    body_section = re.search(r"### Changelog entry.*?\n(.*?)(\n###|$)", description, re.DOTALL)
    if body_section:
        return body_section.group(1).strip()
    return None

def match_pr_to_changelog_category(category):
    categories = {
        "New feature": "Functionality",
        "Experimental feature": "Functionality",
        "Improvement": "Functionality",
        "Performance improvement": "Performance",
        "User Interface": "YDB UI",
        "Bugfix": "Bug fixes",
        "Backward incompatible change": "Backward incompatible change",
        "Documentation (changelog entry is not required)": UNCATEGORIZED,
        "Not for changelog (changelog entry is not required)": UNCATEGORIZED
    }
    if category in categories:
        return categories[category]
    for key, value in categories.items():
        if key.startswith(category):
            return value
    return UNCATEGORIZED


def update_changelog(changelog_path, pr_data):
    changelog = to_dict(changelog_path)
    if UNRELEASED not in changelog:
        changelog[UNRELEASED] = {}

    for pr in pr_data:
        if validate_pr_description(pr["body"], is_not_for_cl_valid=False):
            category = extract_changelog_category(pr["body"])
            category = match_pr_to_changelog_category(category)
            dirty_body = extract_changelog_body(pr["body"])
            body = dirty_body.replace("\r", "")
            if category and body:
                body += f" [#{pr['number']}]({pr['url']})"
                body += f" ([{pr['name']}]({pr['user_url']}))"
                if category not in changelog[UNRELEASED]:
                    changelog[UNRELEASED][category] = {}
                if pr['number'] not in changelog[UNRELEASED][category]:
                    changelog[UNRELEASED][category][pr['number']] = body

    to_file(changelog_path, changelog)

def run_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"::error::Command failed with exit code {e.returncode}: {e.stderr.decode()}")
        print(f"::error::Command: {e.cmd}")
        print(f"::error::Output: {e.stdout.decode()}")
        sys.exit(1)
    return result.stdout.decode().strip()

def branch_exists(branch_name):
    result = subprocess.run(["git", "ls-remote", "--heads", "origin", branch_name], capture_output=True, text=True)
    return branch_name in result.stdout

def fetch_pr_details(pr_id):
    url = f"https://api.github.com/repos/{get_github_api_url()}/pulls/{pr_id}"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {GH_TOKEN}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_user_details(username):
    url = f"https://api.github.com/users/{username}"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {GH_TOKEN}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: update_changelog.py <pr_data_file> <changelog_path> <base_branch> <suffix>")
        sys.exit(1)

    pr_data_file = sys.argv[1]
    changelog_path = sys.argv[2]
    base_branch = sys.argv[3]
    suffix = sys.argv[4]
   
    try:
        with open(pr_data_file, 'r') as file:
            pr_ids = json.load(file)
    except Exception as e:
        print(f"::error::Failed to read or parse PR data file: {e}")
        sys.exit(1)

    pr_data = []
    for pr in pr_ids:
        try:
            pr_details = fetch_pr_details(pr["id"])
            user_details = fetch_user_details(pr_details["user"]["login"])
            name = user_details.get("name", None)
            if validate_pr_description(pr_details["body"], is_not_for_cl_valid=False):
                pr_data.append({
                    "number": pr_details["number"],
                    "body": pr_details["body"].strip(),
                    "url": pr_details["html_url"],
                    "name": name or pr_details["user"]["login"],  # Use login if name is not available
                    "user_url": pr_details["user"]["html_url"]
                })
        except Exception as e:
            print(f"::error::Failed to fetch PR details for PR #{pr['id']}: {e}")
            sys.exit(1)

    update_changelog(changelog_path, pr_data)

    base_branch_name = f"changelog/{base_branch}-{suffix}"
    branch_name = base_branch_name
    index = 1
    while branch_exists(branch_name):
        branch_name = f"{base_branch_name}-{index}"
        index += 1
    run_command(f"git checkout -b {branch_name}")
    run_command(f"git add {changelog_path}")
    run_command(f"git commit -m \"Update CHANGELOG.md for {suffix}\"")
    run_command(f"git push origin {branch_name}")

    pr_title = f"Update CHANGELOG.md for {suffix}"
    pr_body = f"This PR updates the CHANGELOG.md file for {suffix}."
    pr_create_command = f"gh pr create --title \"{pr_title}\" --body \"{pr_body}\" --base {base_branch} --head {branch_name}"
    pr_url = run_command(pr_create_command)
    # run_command(f"gh pr edit {pr_url} --add-assignee galnat") # TODO: Make assignee customizable
