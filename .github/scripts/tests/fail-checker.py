#!/usr/bin/env python3
import argparse
import glob
import os
from typing import List
import xml.etree.ElementTree as ET


def check_for_fail(paths: List[str]):
    for path in paths:
        for fn in glob.glob(os.path.join(path, "*.xml")):
            root = ET.parse(fn).getroot()
            if root.tag != "testsuite":
                suites = root.findall("testsuite")
            else:
                suites = [root]

            for suite in suites:
                if int(suite.get("failures", 0)) > 0:
                    print(f"::error::You have failed tests")
                    raise SystemExit(-1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="+", help="jsuite xml reports directories")
    args = parser.parse_args()
    check_for_fail(args.path)


if __name__ == "__main__":
    main()
