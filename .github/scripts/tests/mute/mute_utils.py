from __future__ import annotations
import os
import re
import xml.etree.ElementTree as ET
import sys
import json

CONFIG_DIR = os.path.join('.github', 'config')
MUTE_UPDATE_BUILD_TYPES_CONFIG = os.path.join(CONFIG_DIR, 'mute_update_build_types.json')

def _normalize_relative_path(path: str) -> str:
    return path.replace('\\', '/')


def _repo_root_from_this_file() -> str:
    # .../<repo>/.github/scripts/tests/mute/mute_utils.py
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))


def _load_muted_ya_path_policy() -> tuple[str, dict[str, str]]:
    cfg_path = os.path.join(_repo_root_from_this_file(), MUTE_UPDATE_BUILD_TYPES_CONFIG)
    with open(cfg_path, encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f'{cfg_path}: expected JSON object')

    raw_default = data.get('default_muted_ya_path')
    if not isinstance(raw_default, str) or not raw_default.strip():
        raise ValueError(f'{cfg_path}: "default_muted_ya_path" must be a non-empty string')
    default_path = _normalize_relative_path(raw_default.strip())

    per_preset: dict[str, str] = {}
    raw_paths = data.get('muted_ya_paths', {})
    if raw_paths is None:
        raw_paths = {}
    if not isinstance(raw_paths, dict):
        raise ValueError(f'{cfg_path}: "muted_ya_paths" must be a JSON object')
    for preset, rel_path in raw_paths.items():
        preset_key = str(preset).strip().lower()
        if not preset_key:
            continue
        if not isinstance(rel_path, str) or not rel_path.strip():
            raise ValueError(f'{cfg_path}: muted_ya_paths["{preset_key}"] must be a non-empty string')
        per_preset[preset_key] = _normalize_relative_path(rel_path.strip())
    return default_path, per_preset


def dedicated_relative(preset: str) -> str:
    preset = preset.strip().lower()
    default_path, per_preset = _load_muted_ya_path_policy()
    return per_preset.get(preset, default_path)


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
    try:
        from junit_utils import add_junit_property
    except ModuleNotFoundError:
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        from junit_utils import add_junit_property

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

def get_previously_skipped_tests(report_json_path: str) -> set[tuple[str, str]]:
    result: set[tuple[str, str]] = set()
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
    import yaml

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
    if len(sys.argv) == 4 and sys.argv[1] == "convert_muted_txt_to_yaml":
        convert_muted_txt_to_yaml(sys.argv[2], sys.argv[3])
    else:
        print(
            "Usage: mute_utils.py convert_muted_txt_to_yaml <muted_txt_path> <report_json_path>",
            file=sys.stderr,
        )
        raise SystemExit(2)
