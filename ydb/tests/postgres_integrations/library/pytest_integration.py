from typing import Dict, List, Set, Optional
import os
from os import path

from dataclasses import dataclass
from enum import Enum

import docker
import xmltodict
import pytest

import yatest


class TestState(Enum):
    PASSED = 1
    FAILED = 2
    SKIPPED = 3

@dataclass
class TestCase:
    name: str
    state: TestState
    log: str


class IntegrationTests:
    _folder: str
    _image_name: str
    _all_tests: Set[str]
    _selected_test: Set[str]
    _docker_executed: bool
    _test_results: Dict[str, TestCase]

    def __init__(self, folder: str, image_name: str = 'ydb-pg-test-image'):
        self._folder = folder
        self._image_name = image_name

        self._all_tests = _read_tests(folder)
        self._selected_test = set(self._all_tests)

        self._docker_executed = False
        self._test_results = dict()

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
        if not self._docker_executed:
            self._run_tests_in_docker(testname)
            test_results_file=path.join(self._test_result_folder, "raw", "result.xml")
            self._test_results = _read_tests_result(test_results_file)

        self._check_test_results(testname)

    def _check_test_results(self, testname: str):
        print(self._test_results)
        test = self._test_results[testname]
        if test.state == TestState.PASSED:
            return
        if test.state == TestState.SKIPPED:
            pytest.skip(test.log)
        if test.state == TestState.FAILED:
            pytest.fail(reason=test.log)

        raise Exception(f"Unexpected test state: '{test.state}'")

    def _run_tests_in_docker(self, test_name: Optional[str]):
        if self._docker_executed:
            return
        self._docker_executed = True

        if test_name is None:
            test_name=""

        client: docker.Client = docker.from_env()


        client.images.build(
            path = self._folder,
            tag=self._image_name,
            network_mode='host',
        )

        try:
            exchange_folder=path.join(yatest.common.output_path(), "exchange")
            os.mkdir(exchange_folder)
        except FileExistsError:
            pass

        try:
            os.mkdir(self._test_result_folder)
        except FileExistsError:
            pass


        # TODO: run YDB with scripts/receipt and get connection port/database with runtime
        container = client.containers.create(
            image=self._image_name,
            # command="/docker-start.bash",
            # detach=True,
            # auto_remove=True,
            environment = [
                "PGUSER=root",
                "PGPASSWORD=1234",
                "PGHOST=ydb",
                "PGPORT=5432",
                "PGDATABASE=local",
                "PQGOSSLTESTS=0",
                "PQSSLCERTTEST_PATH=certs",
                # f"YDB_PG_TESTNAME={test_name}",
            ],
            mounts = [
                docker.types.Mount(
                    target="/exchange",
                    source=exchange_folder,
                    type="bind",
                ),
                docker.types.Mount(
                    target="/test-result",
                    source=self._test_result_folder,
                    type="bind",
                ),
            ],
            network_mode='host',
        )
        try:
            container.start()
            container.wait()
            print(container.logs().decode())
        finally:
            container.remove()


    @property
    def _test_result_folder(self):
        return path.join(yatest.common.output_path(), "test-result")

def _read_tests(folder: str) -> Set[str]:
    with open(path.join(folder, "full-test-list.txt"), "rt") as f:
        all = set(line.strip() for line in f.readlines())

    with open(path.join(folder, "unit-tests.txt"), "rt") as f:
        unit = set(f.readlines())

    test_list_for_run = all - unit
    return test_list_for_run

def _read_tests_result(filepath: str) -> Dict[str, TestCase]:
    with open(filepath, "rt") as f:
        data = f.read()
    d = xmltodict.parse(data)
    testsuites = d["testsuites"]
    test_suite = testsuites["testsuite"]
    test_cases = test_suite["testcase"]

    res: Dict[str, TestCase] = dict()

    for test_case in test_cases:
        name = test_case["@classname"] + "/" + test_case["@name"]
        test_state = TestState.PASSED
        if "failure" in test_case:
            test_state = TestState.FAILED
            log = test_case["failure"].get("#text", "")
        if "skipped" in test_case:
            test_state = TestState.SKIPPED
            log = test_case["skipped"].get("#text", "")
        else:
            test_state = TestState.PASSED
            log = ""

        res[name] = TestCase(
            name=name,
            state=test_state,
            log=log,
        )

    return res
