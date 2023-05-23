#!/usr/bin/env python3
import dataclasses
import argparse
import os
import re
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import List, Optional


def make_id(s):
    # TODO: here
    return s


@dataclasses.dataclass
class Testcase:
    case_id: Optional[str]
    name: str
    classname: str
    time: float
    is_fail: bool
    fail_msg: Optional[str]
    stdout: str
    stderr: str

    @property
    def id(self):
        if self.case_id:
            return self.case_id
        return make_id(self.name)

    @property
    def stdout_fn(self):
        return f"{self.id}-stdout.log"

    @property
    def stderr_fn(self):
        return f"{self.id}-stderr.log"


@dataclasses.dataclass
class Testsuite:
    suite_id: str
    name: str
    tests: int
    failures: int
    time: float
    cases: List[Testcase]

    @property
    def is_fail(self):
        return self.failures > 0


def parse_file(fn):
    suites = []
    with open(fn, "r") as fp:
        tree = ET.parse(fp)
        root = tree.getroot()
        for tsuite in root.iter("testsuite"):
            attrs = tsuite.attrib
            suite = Testsuite(
                suite_id=attrs.get("id"),
                name=attrs["name"],
                tests=int(attrs["tests"]),
                failures=int(attrs["failures"]),
                time=float(attrs["time"]),
                cases=[],
            )

            for tcase in tsuite.iter("testcase"):
                cattrs = tcase.attrib

                failure = tcase.find("failure")
                is_fail = False
                fail_msg = ""
                if failure is not None:
                    is_fail = True
                    fail_msg = failure.attrib["message"]

                stdout = stderr = None
                has_stdout = tcase.find("system-out")
                has_stderr = tcase.find("system-err")

                if has_stdout is not None:
                    stdout = has_stdout.text

                if has_stderr is not None:
                    stderr = has_stderr.text

                suite.cases.append(
                    Testcase(
                        case_id=cattrs.get("id"),
                        name=cattrs["name"],
                        classname=cattrs["classname"],
                        time=float(cattrs["time"]),
                        is_fail=is_fail,
                        fail_msg=fail_msg,
                        stdout=stdout,
                        stderr=stderr,
                    )
                )
            suite.cases.sort(key=lambda c: c.is_fail, reverse=True)
            suites.append(suite)
    return suites


def write_case_log(case: Testcase, out: Path):
    if case.stdout:
        with open(out.joinpath(case.stdout_fn), "wt") as fp:
            fp.write(case.stdout)

    if case.stderr:
        with open(out.joinpath(case.stderr_fn), "wt") as fp:
            fp.write(case.stderr)


def get_resource(name):
    with open(Path(os.path.dirname(__file__)).joinpath("res", name), "rt") as fp:
        return fp.read()


def gen_report(suites, out_path: Path, copy_logs):
    suites.sort(key=lambda s: s.failures, reverse=True)
    html = f"""
<html>
<head>
    <style>{get_resource("style.css")}</style>
</head>
<body>
    <table>
        <thead>
            <tr>
                <th class="title">Title</th>
                <th class="time">Time</th>
                <th class="status">Status</th>
                <th class="stdout">STDOUT</th>
                <th class="stdout">STDERR</th>
            </tr>
        </thead>
        <tbody>
    """

    for suite in suites:
        html += f"""
        <tr class="suite">
            <td class="suite">{suite.name}</td>
            <td>{suite.time:.3f}</td>
            <td class="{'fail' if suite.is_fail else 'ok'}" colspan="3">
                {f'{suite.failures} / {suite.tests}' if suite.is_fail else 'OK'}
            </td>
        </tr>
        """
        for case in suite.cases:
            if copy_logs:
                write_case_log(case, out_path)

            html += f"""
        <tr class="case">
            <td><span class="classname">{case.classname}::</span>{case.name}</td>
            <td>{case.time:.3f}</td>
            <td>
                <span class="{'fail' if case.is_fail else 'ok'}">{f'FAIL' if case.is_fail else 'OK'}</span>
            </td>
            <td>
                {f'<a href="{case.stdout_fn}">stdout</a>' if case.stdout else ''}
            </td>
            <td>
                {f'<a href="{case.stderr_fn}">stderr</a>' if case.stderr else ''}
            </td>
        </tr>
            """

    html += """
        </tbody>
    </table>
</body>
"""

    with open(out_path.joinpath("index.html"), "w") as fp:
        fp.write(html)


def main(args):
    suites = []

    if os.path.isdir(args.src):
        for root, dirs, files in os.walk(args.src):
            for f in files:
                fn = os.path.join(root, f)
                suites.extend(parse_file(fn))
    else:
        suites.extend(parse_file(args.src))

    gen_report(suites, out_path=Path(args.dst), copy_logs=not args.no_copy_logs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-copy-logs", action="store_true", default=False)
    parser.add_argument("src")
    parser.add_argument("dst")

    main(parser.parse_args())
