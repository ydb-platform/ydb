import sys
import re
<<<<<<< HEAD
from typing import Tuple

issue_patterns = [
    r"https://github.com/ydb-platform/[a-z\-]+/issues/\d+",
    r"https://st.yandex-team.ru/[a-zA-Z]+-\d+",
    r"#\d+",
    r"[a-zA-Z]+-\d+"
]

pull_request_template = """
### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

...

### Changelog category <!-- remove all except one -->

* New feature
* Experimental feature
* Improvement
* Performance improvement
* User Interface
* Bugfix 
* Backward incompatible change
* Documentation (changelog entry is not required)
* Not for changelog (changelog entry is not required)
"""
=======
import os
import json
import urllib.parse
from typing import Tuple, Optional
from github import Github, Auth as GithubAuth
from pr_template import (
    ISSUE_PATTERNS,
    PULL_REQUEST_TEMPLATE,
    NOT_FOR_CHANGELOG_CATEGORIES,
    ALL_CATEGORIES
)
>>>>>>> e7739333763 (Add PR comment functionality for test runs)

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

    if pull_request_template.strip() in description.strip():
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
    for_cl_categories = [
        "New feature",
        "Experimental feature",
        "User Interface",
        "Improvement", 
        "Performance improvement",
        "Bugfix",
        "Backward incompatible change"
    ]

    not_for_cl_categories = [
        "Documentation (changelog entry is not required)",
        "Not for changelog (changelog entry is not required)"
    ]
    
    valid_categories = for_cl_categories + not_for_cl_categories

    if not any(cat.startswith(category) for cat in valid_categories):
        txt = f"Invalid Changelog category: {category}"
        print(f"::warning::{txt}")
        return False, txt

    if not is_not_for_cl_valid and any(cat.startswith(category) for cat in not_for_cl_categories):
        txt = f"Category is not for changelog: {category}"
        print(f"::notice::{txt}")
        return False, txt

    if not any(cat.startswith(category) for cat in not_for_cl_categories):
        entry_section = re.search(r"### Changelog entry.*?\n(.*?)(\n###|$)", description, re.DOTALL)
        if not entry_section or len(entry_section.group(1).strip()) < 20:
            txt = "The changelog entry is less than 20 characters or missing."
            print(f"::warning::{txt}")
            return False, txt

        # Check if category is Bugfix (case-insensitive)
        is_bugfix = category_lower == "bugfix"
        if is_bugfix:
            def check_issue_pattern(issue_pattern):
                return re.search(issue_pattern, description)

            if not any(check_issue_pattern(issue_pattern) for issue_pattern in issue_patterns):
                txt = "Bugfix requires a linked issue in the changelog entry"
                print(f"::warning::{txt}")
                return False, txt

    print("PR description is valid.")
    return True, "PR description is valid."

def normalize_app_domain(app_domain: str) -> str:
    """Normalize app domain - remove https:// prefix if present."""
    domain = app_domain.strip()
    if domain.startswith("https://"):
        domain = domain[8:]
    if domain.startswith("http://"):
        domain = domain[7:]
    return domain.rstrip('/')

def generate_test_table(pr_number: int, base_ref: str, app_domain: str) -> str:
    """Generate test execution table with buttons for different build presets and test sizes."""
    domain = normalize_app_domain(app_domain)
    base_url = f"https://{domain}/workflow/trigger"
    repo_env = os.environ.get("GITHUB_REPOSITORY")
    if not repo_env or "/" not in repo_env:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set or malformed (expected 'owner/repo')")
    owner, repo = repo_env.split("/", 1)
    workflow_id = "run_tests.yml"
    return_url = f"https://github.com/{owner}/{repo}/pull/{pr_number}"
    
    build_presets = ["relwithdebinfo", "release-asan", "release-msan", "release-tsan"]
    test_size_combinations = [
        ("small,medium", "Small & Medium"),
        ("large", "Large")
    ]
    
    rows = []
    for build_preset in build_presets:
        cells = []
        
        for test_size, test_size_display in test_size_combinations:
            params = {
                "owner": owner,
                "repo": repo,
                "workflow_id": workflow_id,
                "ref": base_ref,
                "pull_number": f"pull/{pr_number}",
                "build_preset": build_preset,
                "test_size": test_size,
                "test_targets": "ydb/",
                "return_url": return_url
            }
            query_string = "&".join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items()])
            url = f"{base_url}?{query_string}"
            url_ui = f"{base_url}?{query_string}&ui=true"
            
            button_label_encoded = build_preset.replace('-', '_')
            buttons = f"[![▶ {build_preset}](https://img.shields.io/badge/%E2%96%B6_{button_label_encoded}-4caf50?style=flat-square)]({url}) [![⚙️](https://img.shields.io/badge/%E2%9A%99%EF%B8%8F-ff9800?style=flat-square)]({url_ui})"
            cells.append(buttons)
        
        rows.append("| " + " | ".join(cells) + " |")
    
    table = "<!-- test-execution-table -->\n"
    table += "<h3>Run tests</h3>\n\n"
    table += "| Small & Medium | Large |\n"
    table += "|----------------|-------|\n"
    table += "\n".join(rows)
    return table

def generate_backport_table(pr_number: int, app_domain: str) -> str:
    """Generate backport execution table with buttons for different branches."""
    domain = normalize_app_domain(app_domain)
    base_url = f"https://{domain}/workflow/trigger"
    repo_env = os.environ.get("GITHUB_REPOSITORY")
    if not repo_env or "/" not in repo_env:
        raise ValueError("GITHUB_REPOSITORY environment variable is not set or malformed (expected 'owner/repo')")
    owner, repo = repo_env.split("/", 1)
    workflow_id = "cherry_pick_v2.yml"  # Workflow file name
    return_url = f"https://github.com/{owner}/{repo}/pull/{pr_number}"
    
    # Load backport branches from config - no fallback, fail if not found
    workspace = os.environ.get("GITHUB_WORKSPACE")
    if not workspace:
        raise ValueError("GITHUB_WORKSPACE environment variable is not set")
    
    backport_branches_path = os.path.join(workspace, ".github", "config", "backport_branches.json")
    
    if not os.path.exists(backport_branches_path):
        raise FileNotFoundError(f"Backport branches config file not found: {backport_branches_path}")
    
    with open(backport_branches_path, 'r') as f:
        branches = json.load(f)
    
    if not isinstance(branches, list) or len(branches) == 0:
        raise ValueError(f"Invalid backport branches config: expected non-empty list, got {type(branches)}")
    
    print(f"::notice::Loaded {len(branches)} backport branches from {backport_branches_path}")
    
    rows = []
    for branch in branches:
        params = {
            "owner": owner,
            "repo": repo,
            "workflow_id": workflow_id,
            "ref": "main",
            "commits": str(pr_number),
            "target_branches": branch,
            "allow_unmerged": "true",
            "return_url": return_url
        }
        query_string = "&".join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items()])
        url = f"{base_url}?{query_string}"
        url_ui = f"{base_url}?{query_string}&ui=true"
        
        rows.append(f"| [![▶ {branch}](https://img.shields.io/badge/%E2%96%B6_{branch.replace('-', '_')}-4caf50?style=flat-square)]({url}) [![⚙️](https://img.shields.io/badge/%E2%9A%99%EF%B8%8F-ff9800?style=flat-square)]({url_ui}) |")
    
    # Collect all unique branches from all entries (each entry may contain multiple branches separated by comma)
    all_unique_branches = set()
    for branch_entry in branches:
        # Split by comma and strip whitespace
        branch_list = [b.strip() for b in branch_entry.split(',')]
        all_unique_branches.update(branch_list)
    
    # Sort for consistent output
    all_unique_branches_sorted = sorted(all_unique_branches)
    all_branches = ",".join(all_unique_branches_sorted)
    
    # Generate URL for backporting all unique branches
    params_multiple = {
        "owner": owner,
        "repo": repo,
        "workflow_id": workflow_id,
        "ref": "main",
        "commits": str(pr_number),
        "target_branches": all_branches,
        "allow_unmerged": "true",
        "return_url": return_url
    }
    query_string_multiple = "&".join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params_multiple.items()])
    url_multiple_ui = f"{base_url}?{query_string_multiple}&ui=true"
    
    table = "<!-- backport-table -->\n"
    table += "<h3>🔄 Backport</h3>\n\n"
    table += "| Actions |\n"
    table += "|----------|\n"
    table += "\n".join(rows)
    table += "\n\n"
    table += f"[![⚙️ Backport (custom)](https://img.shields.io/badge/%E2%9A%99%EF%B8%8F_Backport_%28custom%29-2196F3?style=flat-square)]({url_multiple_ui})"
    return table

def get_legend() -> str:
    """Get legend text for workflow buttons."""
    return "\n**Legend:**\n\n" \
           "* ▶ - immediately runs the workflow with default parameters\n" \
           "* ⚙️ - opens UI to review and modify parameters before running\n"

def ensure_tables_in_pr_body(pr_body: str, pr_number: int, base_ref: str, app_domain: str, 
                              show_test_table: bool = True, show_backport_table: bool = True) -> Optional[str]:
    """Check if test and backport tables exist in PR body, add them if missing.
    
    Args:
        pr_body: Current PR body
        pr_number: PR number
        base_ref: Base branch reference
        app_domain: Application domain for workflow URLs
        show_test_table: Whether to add test execution table
        show_backport_table: Whether to add backport table
    """
    test_table_marker = "<!-- test-execution-table -->"
    backport_table_marker = "<!-- backport-table -->"
    
    has_test_table = test_table_marker in pr_body
    has_backport_table = backport_table_marker in pr_body
    
    # Check if all requested tables already exist
    if show_test_table and show_backport_table:
        if has_test_table and has_backport_table:
            return None  # Both tables already exist
    elif show_test_table:
        if has_test_table:
            return None  # Test table already exists
    elif show_backport_table:
        if has_backport_table:
            return None  # Backport table already exists
    else:
        return None  # No tables requested
    
    # Generate tables to insert
    test_table = None
    backport_table = None
    if show_test_table and not has_test_table:
        test_table = generate_test_table(pr_number, base_ref, app_domain)
    if show_backport_table and not has_backport_table:
        backport_table = generate_backport_table(pr_number, app_domain)
    
    # If no tables to add, return None
    if not test_table and not backport_table:
        return None
    
    # Check if legend already exists (add if at least one table is present)
    legend = get_legend()
    has_legend = "**Legend:**" in pr_body
    
    # Combine tables side by side using HTML table
    tables_html = ""
    if test_table and backport_table:
        # Both tables - place them side by side using HTML table
        # GitHub markdown supports markdown tables inside HTML table cells
        # Using HTML attributes instead of CSS styles for better compatibility
        tables_html = '<table><tr>\n'
        tables_html += '<td valign="top">'
        tables_html += test_table
        tables_html += '</td>\n'
        tables_html += '<td valign="top">'
        tables_html += backport_table
        tables_html += '</td>\n'
        tables_html += '</tr></table>'
    elif test_table:
        tables_html = test_table
    elif backport_table:
        tables_html = backport_table
    
    # Find insertion point after "Description for reviewers" section
    reviewers_section_marker = "### Description for reviewers"
    
    if reviewers_section_marker not in pr_body:
        # If section not found, add at the end
        if pr_body.strip():
            result = pr_body.rstrip() + "\n\n" + tables_html
            if not has_legend:
                result += legend
            return result
        else:
            result = tables_html
            if not has_legend:
                result += legend
            return result
    
    # Find the end of "Description for reviewers" section (before next ### heading)
    lines = pr_body.split('\n')
    insertion_index = len(lines)  # Default to end
    
    for i, line in enumerate(lines):
        if reviewers_section_marker in line:
            # Look for the next ### heading after this section
            for j in range(i + 1, len(lines)):
                if lines[j].strip().startswith('###') and reviewers_section_marker not in lines[j]:
                    insertion_index = j
                    break
            break
    
    # Insert tables and legend after "Description for reviewers" section
    new_lines = lines[:insertion_index] + [""] + [tables_html]
    if not has_legend:
        new_lines.append(legend)
    new_lines.extend(lines[insertion_index:])
    return '\n'.join(new_lines)

def update_pr_body(pr_number: int, new_body: str) -> None:
    """Update PR body via GitHub API. Raises exception on error."""
    try:
        github_token = os.environ.get("GITHUB_TOKEN")
        github_repo = os.environ.get("GITHUB_REPOSITORY")
        
        if not github_token:
            raise ValueError("GITHUB_TOKEN environment variable is not set")
        
        if not github_repo:
            raise ValueError("GITHUB_REPOSITORY environment variable is not set")
        
        gh = Github(auth=GithubAuth.Token(github_token))
        repo = gh.get_repo(github_repo)
        pr = repo.get_pull(pr_number)
        pr.edit(body=new_body)
        print(f"::notice::Updated PR #{pr_number} body with test and backport tables")
    except Exception as e:
        print(f"::error::Failed to update PR #{pr_number} body: {e}")
        raise

def validate_pr_description_from_file(file_path=None, description=None) -> Tuple[bool, str]:
    try:
        if description is not None:
            # Use provided description directly
            desc = description
        elif file_path:
            with open(file_path, 'r') as file:
                desc = file.read()
        else:
            # Read from stdin if available
            if not sys.stdin.isatty():
                desc = sys.stdin.read()
            else:
                desc = ""
        return check_pr_description(desc)
    except Exception as e:
        txt = f"Failed to validate PR description: {e}"
        print(f"::error::{txt}")
        return False, txt

def validate_pr():
    """Validate PR description."""
    # Read PR body from stdin (passed from action.yaml)
    if sys.stdin.isatty():
        raise ValueError("PR body must be provided via stdin")
    
    pr_body = sys.stdin.read()
    
    # Get PR info from event - required, no fallback
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise ValueError("GITHUB_EVENT_PATH environment variable is not set")
    
    if not os.path.exists(event_path):
        raise FileNotFoundError(f"Event file not found: {event_path}")
    
    with open(event_path, 'r') as f:
        event = json.load(f)
    
    if "pull_request" not in event:
        raise ValueError("Event does not contain pull_request data")
    
    pr_number = event["pull_request"]["number"]
    base_ref = event["pull_request"]["base"]["ref"]
    
    # Use PR body from event if stdin is empty
    if not pr_body:
        pr_body = event["pull_request"].get("body") or ""
    
    # Validate PR description
    is_valid, txt = validate_pr_description_from_file(
        description=pr_body
    )
    
    return is_valid, txt, pr_body, pr_number, base_ref

def add_tables_if_needed(pr_body: str, pr_number: int, base_ref: str):
    """Add test and backport tables to PR body if enabled."""
    show_test_table = os.environ.get("SHOW_RUN_TESTS_IN_PR", "").upper() == "TRUE"
    show_backport_table = os.environ.get("SHOW_BACKPORT_IN_PR", "").upper() == "TRUE"
    
    if not show_test_table and not show_backport_table:
        return  # No tables should be added
    
    app_domain = os.environ.get("APP_DOMAIN")
    if not app_domain:
        raise ValueError("APP_DOMAIN environment variable is not set (required when SHOW_RUN_TESTS_IN_PR=TRUE or SHOW_BACKPORT_IN_PR=TRUE)")
    
    updated_body = ensure_tables_in_pr_body(pr_body, pr_number, base_ref, app_domain, 
                                            show_test_table=show_test_table, 
                                            show_backport_table=show_backport_table)
    if updated_body:
        update_pr_body(pr_number, updated_body)

if __name__ == "__main__":
    # Step 1: Validate PR description
    is_valid, txt, pr_body, pr_number, base_ref = validate_pr()
    
    # Step 2: Add tables if validation passed and feature is enabled
    if is_valid:
        add_tables_if_needed(pr_body, pr_number, base_ref)
    
    # Step 3: Post validation status
    from post_status_to_github import post
    post(is_valid, txt)
    
    if not is_valid:
        sys.exit(1)
