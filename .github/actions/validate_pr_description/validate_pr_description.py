import sys
import re
import os
from typing import Tuple
from pr_template import (
    ISSUE_PATTERNS,
    PULL_REQUEST_TEMPLATE,
    FOR_CHANGELOG_CATEGORIES,
    NOT_FOR_CHANGELOG_CATEGORIES,
    ALL_CATEGORIES,
    get_backport_section
)

def validate_pr_description(description, is_not_for_cl_valid=True) -> bool:
    try:
        result, _  = check_pr_description(description, is_not_for_cl_valid)
        return result
    except Exception as e:
        print(f"::error::Error during validation: {e}")
        return False

def check_pr_description(description, is_not_for_cl_valid=True) -> Tuple[bool, str]:
    if not description.strip():
        txt = "PR description is empty. Please fill it out."
        print(f"::warning::{txt}")
        return False, txt

    if "### Changelog category" not in description and "### Changelog entry" not in description:
        return is_not_for_cl_valid, "Changelog category and entry sections are not found."

    if PULL_REQUEST_TEMPLATE.strip() in description.strip():
            return is_not_for_cl_valid, "Pull request template as is."

    # Extract changelog category section
    category_section = re.search(r"### Changelog category.*?\n(.*?)(\n###|$)", description, re.DOTALL)
    if not category_section:
        txt = "Changelog category section not found."
        print(f"::warning::{txt}")
        return False, txt

    categories = [line.strip('* ').strip() for line in category_section.group(1).splitlines() if line.strip()]

    if len(categories) != 1:
        txt = "Only one category can be selected at a time."
        print(f"::warning::{txt}")
        return False, txt

    category = categories[0]

    if not any(cat.startswith(category) for cat in ALL_CATEGORIES):
        txt = f"Invalid Changelog category: {category}"
        print(f"::warning::{txt}")
        return False, txt

    if not is_not_for_cl_valid and any(cat.startswith(category) for cat in NOT_FOR_CHANGELOG_CATEGORIES):
        txt = f"Category is not for changelog: {category}"
        print(f"::notice::{txt}")
        return False, txt

    if not any(cat.startswith(category) for cat in NOT_FOR_CHANGELOG_CATEGORIES):
        entry_section = re.search(r"### Changelog entry.*?\n(.*?)(\n###|$)", description, re.DOTALL)
        if not entry_section or len(entry_section.group(1).strip()) < 20:
            txt = "The changelog entry is less than 20 characters or missing."
            print(f"::warning::{txt}")
            return False, txt

        if category == "Bugfix":
            def check_issue_pattern(issue_pattern):
                return re.search(issue_pattern, description)

            if not any(check_issue_pattern(issue_pattern) for issue_pattern in ISSUE_PATTERNS):
                txt = "Bugfix requires a linked issue in the changelog entry"
                print(f"::warning::{txt}")
                return False, txt

    print("PR description is valid.")
    return True, "PR description is valid."

def validate_pr_description_from_file(file_path) -> Tuple[bool, str]:
    try:
        if file_path:
            with open(file_path, 'r') as file:
                description = file.read()
        else:
            description = sys.stdin.read()
        return check_pr_description(description)
    except Exception as e:
        txt = f"Failed to validate PR description: {e}"
        print(f"::error::{txt}")
        return False, txt

def add_backport_section_if_needed(description: str, pr_number: int) -> str:
    """Add backport section to PR description after 'Description for reviewers' section if it doesn't exist"""
    # Check if backport section already exists
    if "### Backport" in description:
        return description
    
    # Get backport section with real PR number
    backport_section = get_backport_section(pr_number)
    
    # Find "Description for reviewers" section and insert backport section after it
    # Pattern matches: ### Description for reviewers ... (with optional content and trailing ...)
    desc_pattern = r"(### Description for reviewers.*?)(\n\n\.\.\.|\n\.\.\.|\.\.\.|$)"
    match = re.search(desc_pattern, description, re.DOTALL)
    
    if match:
        # Insert backport section after "Description for reviewers"
        desc_section = match.group(1)
        trailing = match.group(2) if match.group(2) else ""
        
        # Clean up trailing dots if present
        if trailing.strip() == "...":
            trailing = ""
        
        # Add backport section
        if desc_section.strip().endswith("..."):
            desc_section = desc_section.rstrip(".").rstrip(".").rstrip(".").rstrip()
        
        new_section = f"{desc_section}\n\n{backport_section}{trailing}"
        return description.replace(match.group(0), new_section)
    else:
        # If "Description for reviewers" not found, add at the end
        if description.strip().endswith("..."):
            description = description.rstrip(".").rstrip(".").rstrip(".").rstrip()
        return f"{description}\n\n{backport_section}"

if __name__ == "__main__":
    description = sys.stdin.read() if not sys.argv[1:] else open(sys.argv[1]).read()
    
    # Validate PR description first (on original description)
    is_valid, txt = check_pr_description(description)
    
    # Get PR number from environment
    pr_number = None
    if "PR_NUMBER" in os.environ:
        try:
            pr_number = int(os.environ["PR_NUMBER"])
        except (ValueError, TypeError):
            pass
    
    # Add backport section if PR number is available, section doesn't exist, and feature is enabled
    show_additional_info = os.environ.get("SHOW_ADDITIONAL_INFO_IN_PR", "FALSE").upper() == "TRUE"
    if pr_number and "### Backport" not in description and show_additional_info:
        updated_description = add_backport_section_if_needed(description, pr_number)
        
        # Update PR description via GitHub API
        if updated_description != description:
            from github import Github, Auth as GithubAuth
            from github.PullRequest import PullRequest
            import json
            
            try:
                gh = Github(auth=GithubAuth.Token(os.environ["GITHUB_TOKEN"]))
                with open(os.environ["GITHUB_EVENT_PATH"]) as fp:
                    event = json.load(fp)
                pr = gh.create_from_raw_data(PullRequest, event["pull_request"])
                pr.edit(body=updated_description)
                print(f"Added backport section to PR #{pr_number}")
            except Exception as e:
                print(f"Warning: Failed to add backport section: {e}")
    
    from post_status_to_github import post
    post(is_valid, txt)
    if not is_valid:
        sys.exit(1)
