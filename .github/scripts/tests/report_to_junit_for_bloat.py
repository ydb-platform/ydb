#!/usr/bin/env python3
"""
Minimal converter from report.json to junit.xml format.
Used only for test_bloat.py compatibility.
"""

import argparse
import json
import sys
from xml.etree import ElementTree as ET


def convert_report_to_junit_minimal(report_path, junit_path):
    """
    Convert a build results report (JSON) to minimal JUnit XML format.
    This is a simplified version that only includes what test_bloat.py needs:
    - suite names
    - test case names
    - test durations
    """
    with open(report_path, 'r') as f:
        report = json.load(f)
    
    # Group test results by suite
    suites = {}
    
    for result in report.get('results', []):
        if result.get('type') != 'test':
            continue
            
        # Skip chunk entries
        if result.get('chunk'):
            continue
        
        # Build suite key from path and name
        path = result.get('path', '')
        test_name = result.get('name', '')
        suite_key = f"{path}/{test_name}"
        
        if suite_key not in suites:
            suites[suite_key] = {
                'name': suite_key,
                'testcases': []
            }
        
        suites[suite_key]['testcases'].append(result)
    
    # Create root element
    root = ET.Element('testsuites')
    
    # Create testsuite elements
    for suite_key, suite_data in suites.items():
        testcases = suite_data['testcases']
        
        suite_tests = len(testcases)
        suite_time = sum(tc.get('duration', 0) for tc in testcases)
        
        suite_elem = ET.Element('testsuite', {
            'name': suite_data['name'],
            'tests': str(suite_tests),
            'time': f"{suite_time:.6f}"
        })
        
        # Add testcase elements
        for testcase in testcases:
            subtest_name = testcase.get('subtest_name', testcase.get('name', 'unknown'))
            
            tc_elem = ET.Element('testcase', {
                'name': subtest_name,
                'time': f"{testcase.get('duration', 0):.6f}"
            })
            
            suite_elem.append(tc_elem)
        
        root.append(suite_elem)
    
    # Write to file
    tree = ET.ElementTree(root)
    ET.indent(tree, space='  ')
    tree.write(junit_path, encoding='utf-8', xml_declaration=True)


def main():
    parser = argparse.ArgumentParser(
        description='Convert build results report (JSON) to minimal JUnit XML for test_bloat.py'
    )
    parser.add_argument(
        '--report',
        required=True,
        help='Path to report.json from --build-results-report'
    )
    parser.add_argument(
        '--junit',
        required=True,
        help='Path where junit.xml should be written'
    )
    
    args = parser.parse_args()
    
    try:
        convert_report_to_junit_minimal(args.report, args.junit)
    except Exception as e:
        print(f"Error converting report: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
