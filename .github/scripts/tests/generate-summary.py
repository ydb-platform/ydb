#!/usr/bin/env python3
import argparse
import os
import dataclasses
import sys
from typing import Optional, List
from junit_utils import get_property_value, iter_xml_files


@dataclasses.dataclass
class SummaryEntry:
    target: str
    log_url: Optional[str]
    reason = ""
    is_failure: bool = False
    is_error: bool = False
    is_muted: bool = False
    is_skipped: bool = False

    @property
    def display_status(self):
        if self.is_error:
            return "Error"
        elif self.is_failure:
            return "Failure"
        elif self.is_muted:
            return "Muted"
        elif self.is_skipped:
            return "Skipped"

        return "?"


def parse_junit(folder_or_file):
    result = []
    for fn, suite, case in iter_xml_files(folder_or_file):
        is_failure = case.find("failure") is not None
        is_error = case.find("error") is not None
        is_muted = get_property_value(case, "mute") is not None
        is_skipped = is_muted is False and case.find("skipped") is not None

        if any([is_failure, is_muted, is_skipped, is_error]):
            cls, method = case.attrib["classname"], case.attrib["name"]
            log_url = get_property_value(case, "url:Log")
            target = f"{ cls }::{ method }" if cls != method else cls

            result.append(
                SummaryEntry(
                    target=target,
                    log_url=log_url,
                    is_skipped=is_skipped,
                    is_muted=is_muted,
                    is_failure=is_failure,
                    is_error=is_error,
                )
            )
    return result


def generate_summary(summary: List[SummaryEntry]):
    log_icon = ":floppy_disk:"
    mute_icon = ":white_check_mark:"

    text = [
        "| Test  | Muted | Log |",
        "| ----: | :---: | --: |",
    ]

    for entry in summary:
        if entry.log_url:
            log_url = f"[{log_icon}]({entry.log_url})"
        else:
            log_url = ""

        mute_target = mute_icon if entry.is_muted else ""

        text.append(f"| {entry.target} | {mute_target} |  {log_url} |")

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
