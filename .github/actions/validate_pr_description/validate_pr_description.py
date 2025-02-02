import sys
import re

issue_patterns = [
    r"https://github.com/ydb-platform/ydb/issues/\d+",
    r"https://st.yandex-team.ru/[a-zA-Z]+-\d+"
]

def validate_pr_description(description, is_not_for_cl_valid=True):
    try:
        if not description.strip():
            print("::warning::PR description is empty. Please fill it out.")
            return False

        if "### Changelog category" not in description:
            print("::warning::Missing '### Changelog category'.")
            return False

        # Extract changelog category section
        category_section = re.search(r"### Changelog category.*?\n(.*?)(\n###|$)", description, re.DOTALL)
        if not category_section:
            print("::warning::Changelog category section not found.")
            return False

        categories = [line.strip('* ').strip() for line in category_section.group(1).splitlines() if line.strip()]

        if len(categories) != 1:
            print("::warning::Only one category can be selected at a time.")
            return False

        category = categories[0]
        valid_categories = [
            "New feature",
            "Experimental feature",
            "User Interface",
            # "Improvement", # Obsolete category
            "Performance improvement",
            "Bugfix",
            "Backward incompatible change"
        ]

        not_for_cl_categories = [
            "Documentation (changelog entry is not required)",
            "Not for changelog (changelog entry is not required)"
        ]
        
        valid_categories += not_for_cl_categories

        if not any(cat.startswith(category) for cat in valid_categories):
            print(f"::warning::Invalid Changelog category: {category}")
            return False

        if not is_not_for_cl_valid and any(cat.startswith(category) for cat in not_for_cl_categories):
            print(f"::notice::Category is not for changelog: {category}")
            return False

        if not any(cat.startswith(category) for cat in not_for_cl_categories):
            entry_section = re.search(r"### Changelog entry.*?\n(.*?)(\n###|$)", description, re.DOTALL)
            if not entry_section or len(entry_section.group(1).strip()) < 20:
                print("::warning::Changelog entry is too short or missing.")
                return False

            if category == "Bugfix":
                def check_issue_pattern(issue_pattern):
                    return re.search(issue_pattern, description)

                if not any(check_issue_pattern(issue_pattern) for issue_pattern in issue_patterns):
                    print("::warning::Bugfix requires a linked issue in the changelog entry")
                    return False

        print("PR description is valid.")
        return True

    except Exception as e:
        print(f"::error::Error during validation: {e}")
        return False

def validate_pr_description_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            description = file.read()
        return validate_pr_description(description)
    except Exception as e:
        print(f"::error::Failed to validate PR description: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: validate_pr_description.py <path_to_pr_description_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    is_valid = validate_pr_description_from_file(file_path)
    if not is_valid:
        sys.exit(1)
