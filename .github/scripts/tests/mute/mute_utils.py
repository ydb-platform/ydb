from __future__ import annotations
from typing import Set, Tuple
import operator
import os
import re
import shlex
import xml.etree.ElementTree as ET
import sys
import yaml
import json

try:
    from junit_utils import add_junit_property
except ModuleNotFoundError:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from junit_utils import add_junit_property

CONFIG_DIR = os.path.join('.github', 'config')

# Dedicated file per preset (relwithdebinfo keeps historical name muted_ya.txt).
DEDICATED_NAMES: dict[str, str] = {
    'relwithdebinfo': 'muted_ya.txt',
    'debug': 'muted_ya.txt',
    'release': 'muted_ya.txt',
    'release-asan': 'muted_ya_asan.txt',
    'release-tsan': 'muted_ya_tsan.txt',
    'release-msan': 'muted_ya_msan.txt',
}


def dedicated_relative(preset: str) -> str:
    preset = preset.strip().lower()
    fn = DEDICATED_NAMES.get(preset, 'muted_ya.txt')
    return os.path.join(CONFIG_DIR, fn).replace('\\', '/')


def resolve_for_workspace(repo_root: str, preset: str) -> tuple[str, bool]:
    """
    Returns (path relative to repo_root, used_fallback).
    If the dedicated file is missing, use muted_ya.txt (backward compatible).
    """
    rel = dedicated_relative(preset)
    full = os.path.normpath(os.path.join(repo_root, rel))
    if os.path.isfile(full):
        return rel.replace('\\', '/'), False
    fb = os.path.join(repo_root, CONFIG_DIR, 'muted_ya.txt')
    if not os.path.isfile(fb):
        raise FileNotFoundError(f'neither {full} nor fallback {fb} exists')
    return os.path.join(CONFIG_DIR, 'muted_ya.txt').replace('\\', '/'), True


def bash_exports_for_workspace(repo_root: str, preset: str) -> tuple[str, str, str]:
    path, used_fallback = resolve_for_workspace(repo_root, preset)
    fallback_flag = '1' if used_fallback else '0'
    exports = [
        f'export MUTED_YA_FILE={shlex.quote(path)}',
        f'export MUTED_YA_IS_FALLBACK={fallback_flag}',
    ]
    return path, fallback_flag, '\n'.join(exports)


def pattern_to_re(pattern):
    res = []
    for c in pattern:
        if c == '*':
            res.append('.*')
        else:
            res.append(re.escape(c))

    return f"(?:^{''.join(res)}$)"


class MuteTestCheck:
    def __init__(self, fn):
        self.regexps = []

        with open(fn, 'r') as fp:
            for line in fp:
                line = line.strip()
                pattern = pattern_to_re(line)

                try:
                    self.regexps.append(re.compile(pattern))
                except re.error:
                    print(f"Unable to compile regex {pattern!r}")
                    raise

    def __call__(self, fullname):
        for r in self.regexps:
            if r.match(fullname):
                return True
        return False


def mute_target(testcase):
    err_text = []
    err_msg = None
    found = False

    for node_name in ('failure', 'error'):
        while 1:
            err_node = testcase.find(node_name)
            if err_node is None:
                break

            msg = err_node.get('message')
            if msg:
                if err_msg is None:
                    err_msg = msg
                else:
                    err_text.append(msg)

            if err_node.text:
                err_text.append(err_node.text)

            found = True
            testcase.remove(err_node)

    if not found:
        return False

    skipped = ET.Element("skipped")

    if err_msg:
        skipped.set('message', err_msg)

    if err_text:
        skipped.text = '\n'.join(err_text)
    testcase.append(skipped)

    add_junit_property(testcase, "mute", "automatically muted based on rules")

    return True


def remove_failure(node):
    while 1:
        failure = node.find("failure")
        if failure is None:
            break
        node.remove(failure)


def op_attr(node, attr, op, value):
    v = int(node.get(attr, 0))
    node.set(attr, str(op(v, value)))


def inc_attr(node, attr, value):
    return op_attr(node, attr, operator.add, value)


def dec_attr(node, attr, value):
    return op_attr(node, attr, operator.sub, value)


def update_suite_info(root, n_remove_failures=None, n_remove_errors=None, n_skipped=None):
    if n_remove_failures:
        dec_attr(root, "failures", n_remove_failures)

    if n_remove_errors:
        dec_attr(root, "errors", n_remove_errors)

    if n_skipped:
        inc_attr(root, "skipped", n_skipped)


def recalc_suite_info(suite):
    tests = failures = skipped = 0
    elapsed = 0.0

    for case in suite.findall("testcase"):
        tests += 1
        elapsed += float(case.get("time", 0))
        if case.find("skipped"):
            skipped += 1
        if case.find("failure"):
            failures += 1

    suite.set("tests", str(tests))
    suite.set("failures", str(failures))
    suite.set("skipped", str(skipped))
    suite.set("time", str(elapsed))


def _split(s: str, sep: str) -> tuple[str, str]:
    p = s.find(sep)
    if p < 0:
        return s, ''
    else:
        return s[:p], s[p + 1 :]

def get_previously_skipped_tests(report_json_path: str) -> Set[Tuple[str, str]]:
    result = set()
    if report_json_path:
        with open(report_json_path, 'r') as f:
            report = json.load(f)
        for test in report.get('results', []):
            if test.get('status', '') not in {'SKIPPED'}:
                continue
            path = test.get('path', '')
            name = test.get('name', '')
            sub_name = test.get('subtest_name', '')
            if name and sub_name:
                result.add((path, f'{name}.{sub_name}'))
            elif name:
                result.add((path, name))
            elif sub_name:
                result.add((path, sub_name))
    return result

def convert_muted_txt_to_yaml(muted_txt_path: str, report_json_path: str) -> None:
    with open(muted_txt_path) as file:
        muted_tests = file.readlines()
    previously_skipped = get_previously_skipped_tests(report_json_path)
    filter_by_suite: dict[tuple[str, str], list[str]] = {}
    for test_line in [l.strip() for l in muted_tests]:
        if not test_line:
            continue
        path, filter = _split(test_line, ' ')
        if filter.endswith('chunk'):
            continue
        if (path, filter) in previously_skipped:
            continue
        suite_type = ''
        filter = filter.replace('.', '::').replace('::py::', '.py::')

        filter_by_suite.setdefault((path, suite_type), [])
        filter_by_suite[(path, suite_type)].append(filter)

    result = []
    for (path, suite_type), filter in filter_by_suite.items():
        result.append({})
        if path:
            result[-1]['path'] = path
        if suite_type:
            result[-1]['suite_type'] = suite_type
        if filter and '' not in filter:
            if len(filter) == 1:
                result[-1]['test_filter'] = filter[0]
            else:
                result[-1]['test_filter'] = filter
    print(yaml.dump(result))


if __name__ == "__main__":
    args = sys.argv
    globals()[args[1]](*args[2:])
