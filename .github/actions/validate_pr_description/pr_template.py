"""
PR template and categories definitions for YDB project.
Used by both validate_pr_description.py and cherry_pick.py to ensure consistency.
"""
import json
import os
from urllib.parse import quote

# Issue reference patterns for validation
ISSUE_PATTERNS = [
    r"https://github.com/ydb-platform/[a-z\-]+/issues/\d+",
    r"https://st.yandex-team.ru/[a-zA-Z]+-\d+",
    r"#\d+",
    r"[a-zA-Z]+-\d+"
]


def get_backport_branches() -> list[str]:
    """Load backport branches from config file, fallback to default if file doesn't exist"""
    config_path = os.path.join(
        os.path.dirname(__file__),
        '..', '..', 'config', 'backport_branches.json'
    )
    try:
        with open(config_path, 'r') as f:
            branches = json.load(f)
            return branches if isinstance(branches, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        # Fallback to default branches if config file doesn't exist
        return ["stable", "stable_2", "stable_3"]


def get_backport_section(pr_number: int = None) -> str:
    """Generate backport section for PR template based on branches from config
    
    Args:
        pr_number: Optional PR number to use in URLs. If None, uses {{Pull_number}} placeholder.
    """
    branches = get_backport_branches()
    if not branches:
        return ""
    
    # Use actual PR number or placeholder
    if pr_number:
        commits_param = str(pr_number)
    else:
        commits_param = quote("{{Pull_number}}", safe='')
    
    # Generate table rows for each branch
    table_rows = []
    for branch in branches:
        # For single branch, use actual branch name
        target_branches_param = quote(branch, safe='')
        backport_url = f"https://ydb-tech-qa.duckdns.org/workflow/trigger?owner=ydb-platform&repo=ydb&workflow_id=cherry_pick.yml&ref=main&commits={commits_param}&target_branches={target_branches_param}"
        configure_url = f"{backport_url}&ui=true"
        
        # Create badges with shields.io (use underscore instead of space for better compatibility)
        backport_badge_text = "▶_Backport"
        configure_badge_text = "⚙️"
        backport_badge = f"https://img.shields.io/badge/{backport_badge_text}-2196f3?style=flat-square"
        configure_badge = f"https://img.shields.io/badge/{configure_badge_text}-ff9800?style=flat-square"
        
        table_rows.append(f"| `{branch}` | [![▶ Backport]({backport_badge})]({backport_url}) [![⚙️]({configure_badge})]({configure_url}) |")
    
    # Generate "Select branches manually" URL with all branches (comma-separated)
    all_branches_str = ",".join(branches)
    all_branches_encoded = quote(all_branches_str, safe='')
    manual_url = f"https://ydb-tech-qa.duckdns.org/workflow/trigger?owner=ydb-platform&repo=ydb&workflow_id=cherry_pick.yml&ref=main&commits={commits_param}&target_branches={all_branches_encoded}&ui=true"
    
    table_content = "\n".join(table_rows)
    
    return f"""### Backport <!-- (optional) quick actions for backporting this PR -->

📦 **Backport**

| Branch | Actions |
|--------|---------|
{table_content}

[▶ Select branches manually]({manual_url}) - opens UI to select branches

**Legend:**
* ▶ - immediately runs the workflow with default parameters
* ⚙️ - opens UI to review and modify parameters before running"""

# Full PR template (backport section is added separately after Description for reviewers)
PULL_REQUEST_TEMPLATE = """### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

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
* Not for changelog (changelog entry is not required)"""

# Categories that require changelog entry
FOR_CHANGELOG_CATEGORIES = [
    "New feature",
    "Experimental feature",
    "User Interface",
    "Improvement",
    "Performance improvement",
    "Bugfix",
    "Backward incompatible change"
]

# Categories that don't require changelog entry
NOT_FOR_CHANGELOG_CATEGORIES = [
    "Documentation (changelog entry is not required)",
    "Not for changelog (changelog entry is not required)"
]

# All valid categories
ALL_CATEGORIES = FOR_CHANGELOG_CATEGORIES + NOT_FOR_CHANGELOG_CATEGORIES


def get_category_section_template() -> str:
    """Get the category section template as a string (for cherry_pick.py)"""
    return "\n".join([f"* {cat}" for cat in ALL_CATEGORIES])


def get_category_section_for_selected(category: str) -> str:
    """Get category section with only selected category marked"""
    return f"* {category}"

