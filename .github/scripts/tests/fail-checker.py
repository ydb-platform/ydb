#!/usr/bin/env python3
import argparse
import json
import sys
from typing import List


def check_for_fail(paths: List[str], output_path: str):
    failed_list = []
    error_list = []
    
    for path in paths:
        fn = path
        try:
            with open(fn, 'r') as f:
                report = json.load(f)
            
            if not isinstance(report, dict) or not isinstance(report.get("results"), list):
                raise ValueError("Expected a report object with a results list")

            for result in report["results"]:
                if not isinstance(result, dict):
                    raise ValueError("Expected each result to be an object")
                # Filtering (suite, build, configure) is done by transform_build_results.py
                status = result.get("status")
                if not status:
                    continue
                
                error_type = result.get("error_type", "")
                path_str = result.get("path", "")
                name = result.get("name", "")
                subtest_name = result.get("subtest_name", "")
                
                # Format: name.subtest_name, then full_name as {path}/{test_name} (same as generate-summary.py and upload_tests_results.py)
                if subtest_name:
                    if name:
                        test_name = f"{name}.{subtest_name}"
                    else:
                        test_name = subtest_name
                else:
                    test_name = name or ""
                
                # Construct full_name as {path}/{test_name}, matching other scripts
                if path_str:
                    full_name = f"{path_str}/{test_name}" if test_name else path_str
                else:
                    full_name = test_name
                
                # Check for failures and errors
                if status == "FAILED":
                    failed_list.append((full_name, fn))
                elif status == "ERROR":
                    error_list.append((full_name, fn))
        except (OSError, ValueError, KeyError) as e:
            print(f"::error::Unable to read test report {fn}: {e}", file=sys.stderr)
            raise SystemExit(2)
    
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
