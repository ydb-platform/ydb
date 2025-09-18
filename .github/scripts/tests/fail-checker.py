#!/usr/bin/env python3
import argparse
from typing import List
from junit_utils import iter_xml_files


def check_for_fail(paths: List[str], output_path: str):
    failed_list = []
    error_list = []
    for path in paths:
        for fn, suite, case in iter_xml_files(path):
            is_failure = case.find("failure") is not None
            is_error = case.find("error") is not None
            test_name = f"{case.get('classname')}/{case.get('name')}"

            if is_failure:
                failed_list.append((test_name, fn))
            elif is_error:
                error_list.append((test_name, fn))
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
    parser.add_argument("path", nargs="+", help="jsuite xml reports directories")
    args = parser.parse_args()
    check_for_fail(args.path, args.output_path)


if __name__ == "__main__":
    main()
