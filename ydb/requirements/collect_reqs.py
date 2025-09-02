import os
import re
import requests

GITHUB_API_URL = "https://api.github.com"
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"

def parse_requirements(file_path, github_token):
    requirements = []
    current_req = None
    current_section = ""
    current_subsection = ""

    with open(file_path, 'r', encoding='utf-8') as file:
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

        # Identify a GitHub issue
        issue_match = re.match(r"- #(\d+)", line)
        if issue_match:
            issue_number = issue_match.group(1)
            issue_data = fetch_github_issue(issue_number, github_token)
            if issue_data:
                if current_req:
                    requirements.append(current_req)
                issue_id = issue_data.get('node_id')
                sub_issues = fetch_sub_issues_by_id(issue_id, github_token) if issue_id else []
                if issue_data.get('sub_issues_summary'):
                    percent_completed = issue_data['sub_issues_summary']['percent_completed']
                    total = issue_data['sub_issues_summary']['total']
                    completed = issue_data['sub_issues_summary']['completed']
                    if issue_data['sub_issues_summary']['percent_completed'] == 100:
                        status = 'DONE'
                        color = f'rgb(249%2C%20239%2C%20254%2C1)'
                    elif 0 < issue_data['sub_issues_summary']['percent_completed'] < 100 :
                        status = 'PROGRESS'
                        color = f'rgb(254%2C%20248%2C%20202%2C1)'
                    else:
                        status = 'TO%20DO'
                        color = f'rgb(224%2C%20250%2C%20227%2C1)'
                    issue_data['badge'] = f"![{status}](https://img.shields.io/badge/{status}-{completed}%2F{total}:{percent_completed}%25-{color}?style=for-the-badge&logo=database&labelColor=grey)"
                current_req = {
                    'id': f"ISSUE-{issue_number}",
                    'title': issue_data['title'],  # Title of the issue
                    'description': issue_data['body'],
                    'url': issue_data['html_url'],
                    'body': issue_data['body'],
                    'cases': sub_issues,  # Sub-issues as cases
                    'section': current_section,
                    'subsection': current_subsection,
                    'sub_issues_summary': issue_data.get('sub_issues_summary'),
                    'badge': issue_data.get('badge')
                }
            continue

        # Identify a new requirement
        req_match = re.match(r"- \*\*(REQ-[A-Z]+-\d+)\*\*: (.+)", line)
        if req_match:
            if current_req:
                requirements.append(current_req)
            current_req = {
                'id': req_match.group(1),
                'title': req_match.group(2),
                'cases': [],
                'issues': [],
                'section': current_section,
                'subsection': current_subsection
            }
        # Identify requirement description
        #  - **Description**: 
        req_description_match = re.match(r"\s+- \*\*Description\*\*: (.+)", line)
        if req_description_match:
            current_req['description'] = req_description_match.group(1)

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

def fetch_github_issue(issue_number, github_token):
    headers = {"Authorization": f"token {github_token}"}
    response = requests.get(f"{GITHUB_API_URL}/repos/ydb-platform/ydb/issues/{issue_number}", headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch issue #{issue_number}: {response.status_code} {response.text}")
        return None

def fetch_sub_issues_by_id(issue_id, github_token):
    query = """
    query($issueId: ID!, $after: String) {
      node(id: $issueId) {
        ... on Issue {
          subIssues(first: 100, after: $after) {
            nodes {
              title
              number
              url
              id
              body
            }
            pageInfo { 
              hasNextPage 
              endCursor 
            }
          }
        }
      }
    }
    """
    
    variables = {
        "issueId": issue_id,
        "after": None
    }
    
    headers = {
        "Authorization": f"Bearer {github_token}",
        "GraphQL-Features": "sub_issues"
    }
    
    sub_issues = []
    
    while True:
        response = requests.post(GITHUB_GRAPHQL_URL, json={"query": query, "variables": variables}, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            sub_issues_data = data['data']['node']['subIssues']
            nodes = sub_issues_data['nodes']
            for node in nodes:
                sub_issues.append({
                    'case_id': f"#{node['number']}",
                    'name': node['title'],
                    'description': node['body'].split('\n')[0],
                    'path': node['url'],
                    'issue': node['number'],
                    'status': "Pending",
                    'bage': f"[![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/{node['number']})](https://github.com/ydb-platform/ydb/issues/{node['number']})"
                })

            if not sub_issues_data['pageInfo']['hasNextPage']:
                break
            variables['after'] = sub_issues_data['pageInfo']['endCursor']
        else:
            print(f"GraphQL query failed: {response.status_code} {response.text}")
            break

    return sub_issues

def to_anchor(s):
    return '#' + re.sub(r'[\s/:()]+', '-', s.lower().replace('/', '').replace('+', '')).strip('-')


def generate_traceability_matrix(requirements, output_path):
    with open(output_path, 'w', encoding='utf-8') as file:
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

            if req.get('url'):
                file.write(f"#### [{req['id']}]({req['url']}): {req['title']}\n")
            else:
                file.write(f"#### {req['id']}: {req['title']}\n")
            if req.get('badge'):
                linq = to_anchor(f"{req['id']}: {req['title']}")
                file.write(f"[{req['badge']}](./summary.md{linq})\n\n")
            if req['description']:
                file.write(f"**Description**: {req['description']}\n\n")
            if req.get('issues'):
                file.write("Issues:\n")
                for issue in req['issues']:
                    file.write(f"- {issue['id']}: {issue['description']}\n")
                file.write("\n")

            file.write("| Case ID | Name | Description | Issues |  Status |\n")
            file.write("|---------|------|-------------|--------|:--------|\n")
            
            for case in req['cases']:
                issues_list = ""
                if case.get('bage'):
                    issues_list = case['bage']
                if case.get('issues'):
                    issues_list = issues_list + ','.join([f"{issue['bage']}" for issue in case['issues']]) 
                if req.get('issues'):
                    issues_list = issues_list + ','.join([f"{issue['bage']}" for issue in req['issues']] or req['issues'])
                file.write(f"| {case['case_id']} | {case['name']} | {case['description']} | {issues_list} | {case['status']} |\n")
            file.write("\n")
            
def generate_summary(requirements, output_path):
    with open(output_path, 'w', encoding='utf-8') as file:
        file.write("# Summary\n\n")
        section = ''
        subsection = ''
        total = 0
        completed = 0
        for req in requirements:
            if req.get('sub_issues_summary'):
                total += req['sub_issues_summary']['total']
                completed += req['sub_issues_summary']['completed']
        file.write(f"**Completed tests: {completed}/{total}: {round(completed*100/total,2) if total > 0 else 0 }%**\n\n")
        file.write(f"## {req['section']}\n\n")
        for req in requirements:
            if req.get('sub_issues_summary'):
                if section != req['section']:
                    file.write(f"## {req['section']}\n\n")
                    section = req['section']
                if subsection != req['subsection']:
                    file.write(f"### {req['subsection']}\n")
                    subsection = req['subsection']
                
                if req.get('url'):
                    file.write(f"#### [{req['id']}]({req['url']}): {req['title']}\n")
                else:
                    file.write(f"#### {req['id']}: {req['title']}\n")
                if req['description']:
                    file.write(f"**Description**: {req['description']}\n\n")
                if req.get('badge'):
                    linq = to_anchor(f"{req['id']}: {req['title']}")
                    file.write(f"[{req['badge']}](./traceability_matrix.md{linq})\n\n")
                if req.get('issues'):
                    file.write("Issues:\n")
                    for issue in req['issues']:
                        file.write(f"- {issue['id']}: {issue['description']}\n")
                    file.write("\n")

def collect_requirements_from_directory(directory, github_token):
    requirements = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.startswith('req') and file.endswith('.md'):
                file_path = os.path.join(root, file)
                requirements.extend(parse_requirements(file_path, github_token))
    return requirements

def process_and_generate_matrices(base_directory, github_token):
    for root, subdirs, files in os.walk(base_directory):
        # Collect requirements from the current directory and its direct subdirectories
        requirements = collect_requirements_from_directory(root, github_token)

        if requirements:
            matrix_output_file = os.path.join(root, 'traceability_matrix.md')
            summary_output_file = os.path.join(root, 'summary.md')
            generate_traceability_matrix(requirements, matrix_output_file)
            print(f"Generated traceability matrix in {matrix_output_file}")
            generate_summary(requirements, summary_output_file)
            print(f"Generated summary in {summary_output_file}")

if __name__ == "__main__":
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # You need to set this environment variable with your GitHub token
    current_directory = os.path.dirname(os.path.abspath(__file__))
    process_and_generate_matrices(current_directory, GITHUB_TOKEN)
