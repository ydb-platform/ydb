#!/usr/bin/env python3
import argparse
from typing import List
from report_utils import iter_report_results, get_test_name


def check_for_fail(paths: List[str], output_path: str):
    failed_list = []
    error_list = []
    for path in paths:
        for result in iter_report_results(path):
            test_name = get_test_name(result)
            
            if result.get('status') == 'FAILED':
                if result.get('error_type') == 'TIMEOUT':
                    error_list.append((test_name, path))
                else:
                    failed_list.append((test_name, path))
    
    failed_test_count = 0

    for t, fn in failed_list:
        print(f"failure: {t} ({fn})")
        failed_test_count += 1
    for t, fn in error_list:
        print(f"error (timeout): {t} ({fn})")
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
    parser.add_argument("path", nargs="+", help="build results report paths (report.json)")
    args = parser.parse_args()
    check_for_fail(args.path, args.output_path)


if __name__ == "__main__":
    main()
