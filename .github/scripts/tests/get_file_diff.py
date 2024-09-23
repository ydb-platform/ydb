#!/usr/bin/env python3
import argparse
import json
import os
import requests
import subprocess
import sys

def get_diff(base_sha, head_sha, file_path):
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

def extract_diff_lines(base_sha, head_sha, file_path):
    diff = get_diff(base_sha, head_sha, file_path)
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

def main(base_sha, head_sha, file_path):
    added_lines, removed_lines = extract_diff_lines(base_sha, head_sha, file_path)
    if added_lines or removed_lines:
        print(f"file {file_path} changed")
    else:
        print(f"file {file_path} not changed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Returns added and removed lines for file compared by git diff in two commit sha's")
    parser.add_argument('--base_sha', type=str, required= True)
    parser.add_argument('--head_sha', type=str, required= True)
    parser.add_argument('--file_path', type=str, required= True)
    args = parser.parse_args()
    
    main(args.base_sha, args.head_sha, args.file_path)