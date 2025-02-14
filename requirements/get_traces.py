import os
import re

def parse_requirements(file_path):
    requirements = []
    current_req = None

    with open(file_path, 'r') as file:
        lines = file.readlines()

    for line in lines:
        # Identify a new requirement
        req_match = re.match(r"- \*\*(REQ-[A-Z]+-\d+)\*\*: (.+)", line)
        if req_match:
            if current_req:
                requirements.append(current_req)
            current_req = {
                'id': req_match.group(1),
                'description': req_match.group(2),
                'cases': []
            }
        
        # Identify cases with optional paths
        case_match = re.match(r"\s+- Case (\d+\.\d+): \[.+\]\((.+)\) - (.+)", line)
        if case_match and current_req:
            case_id = f"{current_req['id']}-{case_match.group(1)}"
            case_path = case_match.group(2)
            case_desc = case_match.group(3)
            current_req['cases'].append({
                'case_id': case_id,
                'description': case_desc,
                'path': case_path,
                'issue': "N/A",  # Placeholder for the issue, can be replaced if needed
                'status': "Pending"  # Placeholder for status
            })

    if current_req:
        requirements.append(current_req)

    return requirements

def generate_traceability_matrix(requirements, output_path):
    with open(output_path, 'w') as file:
        file.write("# Traceability Matrix\n\n")
        
        for req in requirements:
            file.write(f"## {req['id']}\n")
            file.write(f"**Description**: {req['description']}\n\n")
            file.write("| Case ID | Description | Path | Issue | Test Case Status |\n")
            file.write("|---------|-------------|------|-------|------------------|\n")
            
            for case in req['cases']:
                file.write(f"| {case['case_id']} | {case['description']} | {case['path']} | {case['issue']} | {case['status']} |\n")
            
            file.write("\n")

def collect_requirements_from_directory(directory):
    requirements = []
    for root, _, files in os.walk(directory):
        for file in files:
            if  file.endswith('.md'):
                file_path = os.path.join(root, file)
                requirements.extend(parse_requirements(file_path))
    return requirements

def process_and_generate_matrices(base_directory):
    requirements = []
    for root, subdirs, _ in os.walk(base_directory):
        
        for subdir in subdirs:
            subdir_path = os.path.join(root, subdir)
            req = collect_requirements_from_directory(subdir_path)
            requirements.extend(req)
        
        if requirements:
                output_file = os.path.join(root, 'traceability_matrix.md')
                generate_traceability_matrix(requirements, output_file)
                print(f"Generated traceability matrix in {output_file}")

if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__))
    process_and_generate_matrices(current_directory)