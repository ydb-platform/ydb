#!/usr/bin/env python3

# Tool used to analyze test execution times from build results report.
# - generates treemap visualization of test durations
# - helps identify slow tests

import argparse
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '.github', 'scripts', 'tests'))

from report_utils import get_test_results, get_test_duration, is_test_passed, is_test_failed

import tree_map

THRESHOLD = 5  # sec


def generate(report_path, output_dir):
    test_results = get_test_results(report_path)

    tree_paths = []
    tree_paths.append([["/", "dir", 0]])

    for result in test_results:
        # Skip chunk entries
        if result.get('chunk'):
            continue
            
        path = result.get('path', '')
        test_name = result.get('name', '')
        subtest_name = result.get('subtest_name', test_name)
        duration = get_test_duration(result)
        
        # Split test name to remove parameters
        subtest_name = subtest_name.split("[")[0]

        root_name = "/"
        path_parts = [root_name] + path.split("/") + [test_name, subtest_name]
        path_with_info = [[chunk, "dir", 0] for chunk in path_parts if chunk]
        if len(path_with_info) >= 2:
            path_with_info[-1][1] = "testcase"
            path_with_info[-1][2] = duration
            path_with_info[-2][1] = "testsuite"
        tree_paths.append(path_with_info)

    types = [
        ("dir", "Directory", "#66C2A5"),
        ("testsuite", "Test Suite", "#8DA0CB"),
        ("testcase", "Test Case", "#FC8D62"),
    ]
    tree_map.generate_tree_map_html(
        output_dir, 
        tree_paths, 
        unit_name="sec", 
        factor=1.0, 
        types=types, 
        threshold=THRESHOLD, 
        fix_size_threshold=True
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="""A tool for analyzing tests time

To use it run ya make with '--build-results-report <path>' and report.json file will be generated"""
    )
    parser.add_argument(
        "-r",
        "--report",
        required=True,
        help="Path to report.json",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        required=False,
        default="test_bloat",
        help="Output path for treemap view of test durations",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    generate(args.report, args.output_dir)


if __name__ == "__main__":
    main()
