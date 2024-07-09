from typing import List, Set, Optional
from os import path

import pytest

class IntegrationTests:
    _folder: str
    _all_tests: Set[str]
    _selected_test: Set[str]

    def __init__(self, folder):
        self._folder = folder

        self._all_tests = _read_tests(folder)
        self._selected_test = set(self._all_tests)

    def pytest_generate_tests(self, metafunc: pytest.Metafunc):
        """
        Return tests for run through pytest.
        """
        all_tests = list(self._all_tests)
        all_tests.sort()
        metafunc.parametrize('testname', all_tests, ids=all_tests)

    def pytest_deselected(self, items: List[pytest.Item]):
        for item in items:
            test_name = item.callspec.id
            print("rekby-test: removed", test_name)
            self._selected_test.remove(test_name)

    def execute_test(self, testname: str):
        pass

def _read_tests(folder: str) -> Set[str]:
    with open(path.join(folder, "full-test-list.txt"), "rt") as f:
        all = set(line.strip() for line in f.readlines())

    with open(path.join(folder, "unit-tests.txt"), "rt") as f:
        unit = set(f.readlines())

    test_list_for_run = all - unit
    return test_list_for_run
