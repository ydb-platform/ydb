"""TESTOWNERS resolution for analytics (test_results / testowners uploads)."""

import os

from codeowners import CodeOwners

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.normpath(os.path.join(_MODULE_DIR, '..', '..', '..'))
TESTOWNERS_FILE = os.path.join(_REPO_ROOT, '.github', 'TESTOWNERS')


def sort_codeowners_lines(codeowners_lines):
    def path_specificity(line):
        trimmed_line = line.strip()
        if not trimmed_line or trimmed_line.startswith('#'):
            return -1, -1
        path = trimmed_line.split()[0]
        return len(path.split('/')), len(path)

    return sorted(codeowners_lines, key=path_specificity)


def _suite_path_for_codeowners(test):
    if isinstance(test, dict):
        return test['suite_folder']
    return getattr(test, 'classname', None) or getattr(test, 'suite_folder', '') or ''


def _set_owners_field(test, owners_str: str) -> None:
    if isinstance(test, dict):
        test['owners'] = owners_str
    else:
        test.owners = owners_str


def get_testowners_for_tests(tests_data):
    """
    For each test entry, set owners from repo ``.github/TESTOWNERS`` (``TESTOWNERS_FILE``).

    Mutates each entry in ``tests_data`` and returns the same list (for chaining).

    Same owner string format as test_results: ``;;`` / ``:`` between tokens.

    Entry may be a dict with key ``suite_folder`` (upload pipeline) or any object with
    ``classname`` (e.g. generate-summary.TestResult — same path as suite_folder in JSON).
    """
    with open(TESTOWNERS_FILE, 'r') as file:
        data = file.readlines()
        owners_obj = CodeOwners(''.join(sort_codeowners_lines(data)))
        for test in tests_data:
            target_path = _suite_path_for_codeowners(test)
            owners = owners_obj.of(target_path)
            owners_str = ';;'.join([':'.join(x) for x in owners])
            _set_owners_field(test, owners_str)
        return tests_data
