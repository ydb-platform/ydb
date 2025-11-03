#!/usr/bin/env python3
"""
Utility functions for working with build results reports (report.json).
"""

import json
import glob
import os
from typing import Iterator, Dict, Any, List


def iter_report_results(report_path: str) -> Iterator[Dict[str, Any]]:
    """
    Iterate over test results in a build results report.
    
    Args:
        report_path: Path to report.json file or directory containing report files
        
    Yields:
        Test result dictionaries from the report
    """
    if os.path.isfile(report_path):
        files = [report_path]
    elif os.path.isdir(report_path):
        files = glob.glob(os.path.join(report_path, "report.json"))
        if not files:
            # Try to find any .json files
            files = glob.glob(os.path.join(report_path, "*.json"))
    else:
        files = []
    
    for fn in files:
        try:
            with open(fn, 'r') as f:
                report = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Unable to parse {fn}: {e}")
            continue
        
        for result in report.get('results', []):
            if result.get('type') == 'test' and not result.get('chunk'):
                yield result


def get_test_name(result: Dict[str, Any]) -> str:
    """
    Get the full test name from a result.
    
    Returns:
        Full test name in format: path/suite::subtest
    """
    path = result.get('path', '')
    name = result.get('name', '')
    subtest = result.get('subtest_name', '')
    
    if subtest:
        return f"{path}/{name}::{subtest}"
    return f"{path}/{name}"

