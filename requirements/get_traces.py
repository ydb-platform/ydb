import os
import re

def parse_requirements(file_path):
    requirements = []
    current_req = None

    with open(file_path, 'r') as file:
        lines = file.readlines()

    for line in lines:
        # Detect section headings
        section_match = re.match(r"^##\s(.+)", line)
        if section_match:
            current_section = section_match.group(1)
            continue

        subsection_match = re.match(r"^###\s(.+)", line)
        if subsection_match:
            current_subsection = subsection_match.group(1)
            continue

        # Identify a new requirement
        req_match = re.match(r"- \*\*(REQ-[A-Z]+-\d+)\*\*: (.+)", line)
        if req_match:
            if current_req:
                requirements.append(current_req)
            current_req = {
                'id': req_match.group(1),
                'description': req_match.group(2),
                'cases': [],
                'issues': [],
                'section': current_section,
                'subsection': current_subsection
            }
        
        # Identify requirement issues
        issue_match = re.match(r"\s+- ISSUE:(.+):(.+)", line)
        if issue_match and current_req:
            issue_id = issue_match.group(2).split('/')[-1]
            issue_desc = issue_match.group(1)
            current_req['issues'].append({
                'id': issue_id,
                'description': issue_desc,
                'bage': f"[![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/{issue_id})](https://github.com/ydb-platform/ydb/issues/{issue_id})" })

        # Identify cases with optional paths
        case_match = re.match(r"\s+- Case (\d+\.\d+): \[(.+)\]\((.+)\) - (.+)", line)
        if case_match and current_req:
            current_case = {
                'case_id': f"{current_req['id']}-{case_match.group(1)}",
                'name': case_match.group(2),
                'description': case_match.group(4),
                'path': case_match.group(3),
                'issues': [],
                'status': "Pending"
            }
            current_req['cases'].append(current_case)

        # Identify case issues
        case_issue_match = re.match(r"\s{6}- ISSUE:(.+):(.+)", line)
        if case_issue_match and current_case:
            issue_id = issue_match.group(2).split('/')[-1]
            issue_desc = issue_match.group(1)
            current_req['issues'].append({
                'id': issue_id,
                'description': issue_desc,
                'bage': f"[![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/{issue_id})](https://github.com/ydb-platform/ydb/issues/{issue_id})" })

    if current_req:
        requirements.append(current_req)

    return requirements

def generate_traceability_matrix(requirements, output_path):
    with open(output_path, 'w') as file:
        file.write("# Traceability Matrix\n\n")
        section = ''
        subsection = ''
        for req in requirements:
            if section != req['section']:            
                file.write(f"## {req['section']}\n\n")
                section = req['section']
            if subsection != req['subsection']:
                file.write(f"### {req['subsection']}\n")
                subsection = req['subsection']
            file.write(f"#### {req['id']}\n")
            file.write(f"Description: {req['description']}\n\n")

            if req['issues']:
                file.write("Issues:\n")
                for issue in req['issues']:
                    file.write(f"- {issue['id']}: {issue['description']}\n")
                file.write("\n")

            file.write("| Case ID | Name | Description | Issues | Test Case Status |\n")
            file.write("|---------|------|-------------|--------|------------------|\n")

            for case in req['cases']:
                if case['issues'] or req['issues']:
                    issues_list = ','.join([f"{issue['bage']}" for issue in case['issues']]) +  ','.join([f"{issue['bage']}" for issue in req['issues']])
             
                else:
                    issues_list = ""
                file.write(f"| {case['case_id']} | {case['name']} | {case['description']} | {issues_list} | {case['status']} |\n")
            
            file.write("\n")

def collect_requirements_from_directory(directory):
    requirements = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.startswith('req') and file.endswith('.md'):
                file_path = os.path.join(root, file)
                requirements.extend(parse_requirements(file_path))
    return requirements

def process_and_generate_matrices(base_directory):
    for root, subdirs, files in os.walk(base_directory):
        # Collect requirements from the current directory and its direct subdirectories
        requirements = collect_requirements_from_directory(root)

        if requirements:
            output_file = os.path.join(root, 'traceability_matrix.md')
            generate_traceability_matrix(requirements, output_file)
            print(f"Generated traceability matrix in {output_file}")

if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__))
    process_and_generate_matrices(current_directory)
