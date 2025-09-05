#!/usr/bin/env python3

"""
Shared utilities for working with GitHub issues and parsing test names from issue bodies.
Used by both the muted test analytics and issue management scripts.
"""

import re


def parse_body(body):
    """Parse GitHub issue body to extract test names and branches
    
    Args:
        body (str): The GitHub issue body text
        
    Returns:
        tuple: (tests, branches) - lists of test names and branch names
    """
    tests = []
    branches = []
    prepared_body = ''
    start_mute_list = "<!--mute_list_start-->"
    end_mute_list = "<!--mute_list_end-->"
    start_branch_list = "<!--branch_list_start-->"
    end_branch_list = "<!--branch_list_end-->"

    # Extract tests
    if all(x in body for x in [start_mute_list, end_mute_list]):
        idx1 = body.find(start_mute_list)
        idx2 = body.find(end_mute_list)
        lines = body[idx1 + len(start_mute_list) + 1 : idx2].split('\n')
    else:
        if body.startswith('Mute:'):
            prepared_body = body.split('Mute:', 1)[1].strip()
        elif body.startswith('Mute'):
            prepared_body = body.split('Mute', 1)[1].strip()
        elif body.startswith('ydb'):
            prepared_body = body
        lines = prepared_body.split('**Add line to')[0].split('\n')
    tests = [line.strip() for line in lines if line.strip().startswith('ydb/')]

    # Extract branches
    if all(x in body for x in [start_branch_list, end_branch_list]):
        idx1 = body.find(start_branch_list)
        idx2 = body.find(end_branch_list)
        branches = [branch.strip() for branch in body[idx1 + len(start_branch_list) + 1 : idx2].split('\n') if branch.strip()]
    else:
        branches = ['main']

    return tests, branches


def create_test_issue_mapping(issues_data):
    """Create a mapping from test names to GitHub issue information
    
    Args:
        issues_data (list): List of issue dictionaries with 'body', 'url', 'title', 'issue_number' fields
        
    Returns:
        dict: Mapping from test name to list of issue information
    """
    test_to_issue = {}
    
    for issue in issues_data:
        body = issue.get('body', '')
        url = issue.get('url', '')
        
        if not body or not url:
            continue
            
        try:
            # Use the parse_body function to extract tests and branches
            tests, branches = parse_body(body)
            
            for test in tests:
                if test not in test_to_issue:
                    test_to_issue[test] = []
                test_to_issue[test].append({
                    'url': url,
                    'title': issue.get('title', ''),
                    'issue_number': issue.get('issue_number', 0),
                    'state': issue.get('state', ''),
                    'created_at': issue.get('created_at', 0),
                    'branches': branches
                })
        except Exception as e:
            print(f"Warning: Could not parse issue body for issue {url}: {e}")
            continue
    
    return test_to_issue