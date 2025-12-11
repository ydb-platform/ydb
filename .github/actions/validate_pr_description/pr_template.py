"""
PR template and categories definitions for YDB project.
Used by both validate_pr_description.py and cherry_pick.py to ensure consistency.
"""

# Issue reference patterns for validation
ISSUE_PATTERNS = [
    r"https://github.com/ydb-platform/[a-z\-]+/issues/\d+",
    r"https://st.yandex-team.ru/[a-zA-Z]+-\d+",
    r"#\d+",
    r"[a-zA-Z]+-\d+"
]

# Full PR template
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

