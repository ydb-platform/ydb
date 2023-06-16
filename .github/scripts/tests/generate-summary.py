#!/usr/bin/env python3
import argparse
import os
import glob
import dataclasses
import sys
from typing import Optional, List
from xml.etree import ElementTree as ET
from junit_utils import get_property_value


@dataclasses.dataclass
class SummaryEntry:
    target: str
    log_url: Optional[str]
    reason = ""
    is_failure: bool = False
    is_error: bool = False
    is_skipped: bool = False


def iter_xml_files(folder_or_file):
    if os.path.isfile(folder_or_file):
        files = [folder_or_file]
    else:
        files = glob.glob(os.path.join(folder_or_file, "*.xml"))

    for fn in files:
        tree = ET.parse(fn)
        root = tree.getroot()

        if root.tag == "testsuite":
            suites = [root]
        elif root.tag == "testsuites":
            suites = root.findall("testsuite")
        else:
            raise ValueError(f"Invalid root tag {root.tag}")
        for suite in suites:
            for case in suite.findall("testcase"):
                yield fn, suite, case


def parse_junit(folder_or_file):
    result = []
    for fn, suite, case in iter_xml_files(folder_or_file):
        is_failure = case.find("failure") is not None
        is_error = case.find("error") is not None
        is_skipped = case.find("skipped") is not None
        if any([is_failure, is_skipped, is_error]):
            cls, method = case.attrib["classname"], case.attrib["name"]
            log_url = get_property_value(case, "url:Log")
            target = f"{ cls }::{ method }" if cls != method else cls

            result.append(
                SummaryEntry(
                    target=target,
                    log_url=log_url,
                    is_skipped=is_skipped,
                    is_failure=is_failure,
                    is_error=is_error,
                )
            )
    return result


def generate_summary(summary: List[SummaryEntry]):
    icon = ":floppy_disk:"
    mute_icon = ":white_check_mark:"
    text = [
        "| Test  | Status | Muted | Log |",
        "| ----: | :----: | :---: | --: |",
    ]

    for entry in summary:
        mute_target = mute_icon if entry.is_skipped else ""
        if entry.is_error:
            display_reason = "Error"
        elif entry.is_failure:
            display_reason = "Failure"
        else:
            display_reason = ""

        if entry.log_url:
            log_url = f"[{icon}]({entry.log_url})"
        else:
            log_url = ""

        text.append(f"| {entry.target} | {display_reason} | {mute_target} |  {log_url} |")

    return text


def write_summary(title, lines: List[str]):
    summary_fn = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_fn:
        fp = open(summary_fn, "at")
    else:
        fp = sys.stdout

    if title:
        fp.write(f"{title}\n")
    for line in lines:
        fp.write(f"{line}\n")
    fp.write("\n")

    if summary_fn:
        fp.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--title")
    parser.add_argument("folder_or_file")

    args = parser.parse_args()

    summary = parse_junit(args.folder_or_file)

    if summary:
        text = generate_summary(summary)
        write_summary(args.title, text)


if __name__ == "__main__":
    main()
