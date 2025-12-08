#!/usr/bin/env python3

# Tool used to analyze build-results-report. Generates treemap of test durations.

import argparse
import json

import tree_map

THRESHOLD = 5  # sec


def generate(report_path, output_dir):
    with open(report_path, 'r') as f:
        report = json.load(f)

    tree_paths = []
    tree_paths.append([["/", "dir", 0]])

    for result in report.get("results", []):
        if result.get("type") != "test":
            continue
        
        suite_name = result.get("path", "")
        subtest_name = result.get("subtest_name", "")
        
        # Get duration from metrics
        metrics = result.get("metrics", {})
        duration = metrics.get("elapsed_time", 0)
        
        test_name = subtest_name if subtest_name else ""
        # Remove parameterized test parameters
        test_name = test_name.split("[")[0]

        root_name = "/"
        path = [root_name] + suite_name.split("/") + [test_name] if test_name else [root_name] + suite_name.split("/")
        path_with_info = [[chunk, "dir", 0] for chunk in path]
        if test_name:
            path_with_info[-1][1] = "testcase"
            path_with_info[-1][2] = duration
            path_with_info[-2][1] = "testsuite"
        else:
            # If no subtest_name, the last element is the suite
            path_with_info[-1][1] = "testsuite"
            path_with_info[-1][2] = duration
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
        description="""A tool for analyzing tests time\n

To use it run ya make with '--build-results-report <path>' and report JSON file will be generated"""
    )
    parser.add_argument(
        "--build-results-report",
        dest="build_results_report",
        required=True,
        help="Path to build-results-report JSON file",
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
    generate(args.build_results_report, args.output_dir)


if __name__ == "__main__":
    main()
