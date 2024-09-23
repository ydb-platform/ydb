#!/usr/bin/env python3
import os
import sys
import json
import subprocess
import requests

def get_diff(file_path):
    # Get the pull request information from the environment variables
    event_path = os.getenv('GITHUB_EVENT_PATH')
    with open(event_path, 'r') as f:
        event = json.load(f)

    base_sha = event['pull_request']['base']['sha']
    head_sha = event['pull_request']['head']['sha']
    print(f"base_sha: {base_sha}")
    print(f"head_sha: {head_sha}")
    print(f"file_path: {file_path}")
    
    # Use git to get the diff
    result = subprocess.run(
        ['git', 'diff', base_sha, head_sha, '--', file_path],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Error running git diff: {result.stderr}")
    
    return result.stdout

def extract_diff_lines(file_path):
    diff = get_diff(file_path)
    added_lines = []
    removed_lines = []
    for line in diff.splitlines():
        if line.startswith('+') and not line.startswith('+++'):
            added_lines.append(line[1:])
        elif line.startswith('-') and not line.startswith('---'):
            removed_lines.append(line[1:])
    print("\n### Added Lines:")
    print("\n".join(added_lines))
    print("\n### Removed Lines:")
    print("\n".join(removed_lines))
    return added_lines, removed_lines

def main(file_name):
    added_lines, removed_lines = extract_diff_lines(file_name)
    if added_lines or removed_lines:
        print(f"file {file_name} changed")
    else:
        print(f"file {file_name} not changed")

if __name__ == "__main__":
    if len(sys.argv) != 2:

        print("Usage: python get_diff.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    main(filename)