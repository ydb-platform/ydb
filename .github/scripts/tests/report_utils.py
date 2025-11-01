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


def get_test_results(report_path: str) -> List[Dict[str, Any]]:
    """
    Get all test results from a build results report.
    
    Args:
        report_path: Path to report.json file or directory
        
    Returns:
        List of test result dictionaries
    """
    return list(iter_report_results(report_path))


def is_test_failed(result: Dict[str, Any]) -> bool:
    """Check if a test result represents a failure."""
    return result.get('status') == 'FAILED'


def is_test_timeout(result: Dict[str, Any]) -> bool:
    """Check if a test result represents a timeout."""
    return result.get('error_type') == 'TIMEOUT'


def is_test_skipped(result: Dict[str, Any]) -> bool:
    """Check if a test result was skipped."""
    return result.get('status') == 'SKIPPED'


def is_test_passed(result: Dict[str, Any]) -> bool:
    """Check if a test passed."""
    return result.get('status') == 'OK'


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


def get_test_duration(result: Dict[str, Any]) -> float:
    """Get test duration in seconds."""
    return result.get('duration', 0.0)


def get_test_snippet(result: Dict[str, Any]) -> str:
    """Get test error snippet/message."""
    return result.get('rich-snippet', result.get('snippet', ''))
