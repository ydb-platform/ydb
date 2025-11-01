#!/usr/bin/env python3

"""
Convert report.json (from --build-results-report) to junit.xml format.
This maintains compatibility with existing junit-based tooling while
enabling access to additional information like timeout detection.
"""

import argparse
import json
import sys
from xml.etree import ElementTree as ET


def is_timeout_error(testcase):
    """Check if a testcase failed due to timeout."""
    return testcase.get('status') == 'FAILED' and testcase.get('error_type') == 'TIMEOUT'


def is_regular_failure(testcase):
    """Check if a testcase failed for a non-timeout reason."""
    return testcase.get('status') == 'FAILED' and testcase.get('error_type') != 'TIMEOUT'


def convert_report_to_junit(report_path, junit_path):
    """
    Convert a build results report (JSON) to JUnit XML format.
    
    Args:
        report_path: Path to the report.json file from --build-results-report
        junit_path: Path where the junit.xml file should be written
    """
    with open(report_path, 'r') as f:
        report = json.load(f)
    
    # Group test results by suite
    suites = {}
    
    for result in report.get('results', []):
        if result.get('type') != 'test':
            continue
            
        # Skip chunk entries (they represent the whole suite, not individual tests)
        if result.get('chunk'):
            continue
        
        # Build suite key from path and name
        path = result.get('path', '')
        test_name = result.get('name', '')
        suite_key = f"{path}::{test_name}"
        
        if suite_key not in suites:
            suites[suite_key] = {
                'path': path,
                'name': test_name,
                'testcases': []
            }
        
        suites[suite_key]['testcases'].append(result)
    
    # Create root element
    root = ET.Element('testsuites')
    
    total_tests = 0
    total_failures = 0
    total_errors = 0
    total_skipped = 0
    total_time = 0.0
    
    # Create testsuite elements
    for suite_key, suite_data in suites.items():
        testcases = suite_data['testcases']
        
        suite_tests = len(testcases)
        suite_failures = sum(1 for tc in testcases if is_regular_failure(tc))
        suite_errors = sum(1 for tc in testcases if is_timeout_error(tc))
        suite_skipped = sum(1 for tc in testcases if tc.get('status') == 'SKIPPED')
        suite_time = sum(tc.get('duration', 0) for tc in testcases)
        
        suite_elem = ET.Element('testsuite', {
            'name': suite_data['name'],
            'tests': str(suite_tests),
            'failures': str(suite_failures),
            'errors': str(suite_errors),
            'skipped': str(suite_skipped),
            'time': f"{suite_time:.6f}"
        })
        
        # Add testcase elements
        for testcase in testcases:
            # Note: path separator is '/' because these are test paths, not filesystem paths
            classname = f"{suite_data['path']}/{suite_data['name']}"
            subtest_name = testcase.get('subtest_name', testcase.get('name', 'unknown'))
            
            tc_elem = ET.Element('testcase', {
                'classname': classname,
                'name': subtest_name,
                'time': f"{testcase.get('duration', 0):.6f}"
            })
            
            # Add properties
            props = ET.SubElement(tc_elem, 'properties')
            
            # Add test size
            if 'size' in testcase:
                prop = ET.SubElement(props, 'property', {
                    'name': 'test_size',
                    'value': testcase['size']
                })
            
            # Add timeout information if present
            error_type = testcase.get('error_type')
            if error_type:
                prop = ET.SubElement(props, 'property', {
                    'name': 'error_type',
                    'value': error_type
                })
            
            # Add links if present
            links = testcase.get('links', {})
            for link_name, link_paths in links.items():
                if link_paths:
                    # Use the first link if multiple are present
                    link_path = link_paths[0] if isinstance(link_paths, list) else link_paths
                    prop = ET.SubElement(props, 'property', {
                        'name': f'url:{link_name}',
                        'value': link_path
                    })
            
            # Add failure or error element based on status
            status = testcase.get('status', 'OK')
            if status == 'FAILED':
                snippet = testcase.get('rich-snippet', testcase.get('snippet', ''))
                
                # If timeout, use error element; otherwise use failure element
                if error_type == 'TIMEOUT':
                    error_elem = ET.SubElement(tc_elem, 'error', {
                        'type': 'timeout',
                        'message': f'Test timed out after {testcase.get("duration", 0):.2f} seconds'
                    })
                    error_elem.text = snippet
                else:
                    failure_elem = ET.SubElement(tc_elem, 'failure', {
                        'type': error_type or 'test failure',
                        'message': 'Test failed'
                    })
                    failure_elem.text = snippet
            elif status == 'SKIPPED':
                ET.SubElement(tc_elem, 'skipped')
            
            suite_elem.append(tc_elem)
        
        root.append(suite_elem)
        
        total_tests += suite_tests
        total_failures += suite_failures
        total_errors += suite_errors
        total_skipped += suite_skipped
        total_time += suite_time
    
    # Update root attributes
    root.set('tests', str(total_tests))
    root.set('failures', str(total_failures))
    root.set('errors', str(total_errors))
    root.set('skipped', str(total_skipped))
    root.set('time', f"{total_time:.6f}")
    
    # Write to file
    tree = ET.ElementTree(root)
    ET.indent(tree, space='  ')
    tree.write(junit_path, encoding='utf-8', xml_declaration=True)
    
    print(f"Converted {total_tests} test results to {junit_path}")
    print(f"  Passed: {total_tests - total_failures - total_errors - total_skipped}")
    print(f"  Failed: {total_failures}")
    print(f"  Errors (including timeouts): {total_errors}")
    print(f"  Skipped: {total_skipped}")


def main():
    parser = argparse.ArgumentParser(
        description='Convert build results report (JSON) to JUnit XML format'
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
        convert_report_to_junit(args.report, args.junit)
    except Exception as e:
        print(f"Error converting report: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
