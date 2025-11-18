import sys
import re
from typing import Tuple
from pr_template import (
    ISSUE_PATTERNS,
    PULL_REQUEST_TEMPLATE,
    FOR_CHANGELOG_CATEGORIES,
    NOT_FOR_CHANGELOG_CATEGORIES,
    ALL_CATEGORIES
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

if __name__ == "__main__":
    is_valid, txt = validate_pr_description_from_file(sys.argv[1] if len(sys.argv) > 1 else None)
    from post_status_to_github import post
    post(is_valid, txt)
    if not is_valid:
        sys.exit(1)
