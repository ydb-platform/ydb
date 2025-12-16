#!/usr/bin/env python3
import argparse
import json
import os
from typing import List


def check_for_fail(paths: List[str], output_path: str):
    failed_list = []
    error_list = []
    
    for path in paths:
        if not os.path.isfile(path):
            print(f"Warning: Expected a file, but got: {path}", file=__import__('sys').stderr)
            continue
        
        fn = path
        try:
            with open(fn, 'r') as f:
                report = json.load(f)
            
            for result in report.get("results", []):
                if result.get("type") != "test":
                    continue
                
                status = result.get("status", "")
                error_type = result.get("error_type", "")
                path_str = result.get("path", "")
                name = result.get("name", "")
                subtest_name = result.get("subtest_name", "")
                
                # Format: path/name/subtest_name
                test_name = path_str
                if name:
                    test_name = f"{path_str}/{name}"
                if subtest_name:
                    test_name = f"{path_str}/{name}/{subtest_name}" if name else f"{path_str}/{subtest_name}"
                
                # Check for failures and errors
                if status == "FAILED":
                    if error_type == "REGULAR":
                        failed_list.append((test_name, fn))
                    else:
                        error_list.append((test_name, fn))
                elif status == "ERROR":
                    error_list.append((test_name, fn))
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Unable to parse {fn}: {e}", file=__import__('sys').stderr)
            continue
    
    failed_test_count = 0

    for t, fn in failed_list:
        print(f"failure: {t} ({fn})")
        failed_test_count += 1
    for t, fn in error_list:
        print(f"error: {t} ({fn})")
        failed_test_count += 1

    if output_path:
        with open(output_path, "w") as f:
            f.write("{}".format(failed_test_count))

    if failed_test_count:
        print(f"::error::You have failed tests")
        raise SystemExit(-1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--output_path",
        metavar="OUTPUT",
        help=(
            "Output file with count of failed tests"
        ),
    )
    parser.add_argument("path", nargs="+", help="build-results-report JSON files")
    args = parser.parse_args()
    check_for_fail(args.path, args.output_path)


if __name__ == "__main__":
    main()
