#!/usr/bin/env python3
import argparse
import json
import os
import requests
import subprocess
import sys


def get_diff_lines_of_file(base_sha, head_sha, file_path):
    print(f"base_sha: {base_sha}")
    print(f"head_sha: {head_sha}")
    print(f"file_path: {file_path}")

    # Use git to get two versions of file
    result_base = subprocess.run(['git', 'show', base_sha + ':' + file_path], capture_output=True, text=True)
    if result_base.returncode != 0:
        raise RuntimeError(f"Error running git show: {result_base.stderr}")

    result_head = subprocess.run(['git', 'show', head_sha + ':' + file_path], capture_output=True, text=True)
    if result_head.returncode != 0:
        raise RuntimeError(f"Error running git show: {result_base.stderr}")

    base_set_lines = set([line for line in result_base.stdout.splitlines() if line])
    head_set_lines = set([line for line in result_head.stdout.splitlines() if line])
    added_lines = list(head_set_lines - base_set_lines)
    removed_lines = list(base_set_lines - head_set_lines)
    print("\n### Added Lines:")
    print("\n".join(added_lines))
    print("\n### Removed Lines:")
    print("\n".join(removed_lines))
    return added_lines, removed_lines


def main(base_sha, head_sha, file_path):
    added_lines, removed_lines = get_diff_lines_of_file(base_sha, head_sha, file_path)
    if added_lines or removed_lines:
        print(f"file {file_path} changed")
    else:
        print(f"file {file_path} not changed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Returns added and removed lines for file compared by git diff in two commit sha's"
    )
    parser.add_argument('--base_sha', type=str, required=True)
    parser.add_argument('--head_sha', type=str, required=True)
    parser.add_argument('--file_path', type=str, required=True)
    args = parser.parse_args()

    main(args.base_sha, args.head_sha, args.file_path)