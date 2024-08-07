from typing import Callable, Dict, List, Set, Optional
import os
from os import path

from dataclasses import dataclass
from enum import Enum

import docker
import xmltodict
import pytest

import yatest

import logging

from ydb.tests.library.harness.daemon import DaemonError
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_runner import KiKiMR

class TestState(Enum):
    PASSED = 1
    FAILED = 2
    SKIPPED = 3

@dataclass
class TestCase:
    name: str
    state: TestState
    log: str

_filter_format_function = Callable[[List[str]], str]

class IntegrationTests:
    _folder: str
    _image_name: str
    _all_tests: List[str]
    _selected_test: Set[str]
    _docker_executed: bool
    _test_results: Dict[str, TestCase]
    _kikimr_factory: KiKiMR

    def __init__(self, folder: str, image_name: str = 'ydb-pg-test-image'):
        self._folder = folder
        self._image_name = image_name

        self._all_tests = _read_tests(folder)  # sorted test list
        self._selected_test = []

        self._docker_executed = False
        self._test_results = dict()
        self._kikimr_factory = kikimr_cluster_factory()
        self._filter_formatter: Optional[_filter_format_function] = None

    def tearup(self):
        filter = self._filter_formatter(self._selected_test)
        self._run_tests_in_docker(filter)
        print(f"rekby docker test finished")

        test_results_file=path.join(self._test_result_folder, "raw", "result.xml")
        self._test_results = _read_tests_result(test_results_file)
        print(f"rekby result parsed tests:\n\n{self._test_results}")


    def teardown(self):
        pass

    def set_filter_formatter(self, f: _filter_format_function):
        print("rekby, set filter formatter")
        self._filter_formatter = f

    def set_selected_items(self, selected_items: List[pytest.Function]):
        print("rekby set selected items: ", selected_items)
        selected_tests = []
        for item in selected_items:
            print(f"rekby, selected item name: '{item.name}'", )
            if item.name.startswith("test_pg_generated["):
                print("rekby selected test item:", item)
                test_name = item.callspec.id
                print(f"rekby selected test: {test_name}")
                selected_tests.append(test_name)
        selected_tests.sort()
        self._selected_test = selected_tests

    def pytest_generate_tests(self, metafunc: pytest.Metafunc):
        """
        Return tests for run through pytest.
        """
        metafunc.parametrize('testname', self._all_tests, ids=self._all_tests)

    def execute_test(self, testname: str):
        self._check_test_results(testname)

    def _check_test_results(self, testname: str):
        try:
            test = self._test_results[testname]
        except KeyError:
            pytest.fail("test result not found, may be the test was not run")

        if test.state == TestState.PASSED:
            logging.getLogger().log(logging.INFO, test.log)
            return
        if test.state == TestState.SKIPPED:
            logging.getLogger().log(logging.INFO, test.log)
            pytest.skip()
        if test.state == TestState.FAILED:
            logging.getLogger().log(logging.ERROR, test.log)
            pytest.fail()

        raise Exception(f"Unexpected test state: '{test.state}'")

    def _run_tests_in_docker(self, test_filter: str):
        if self._docker_executed:
            return
        self._docker_executed = True
        print("rekby start docker tests")

        try:
            print("rekby, start ydbd")
            self._kikimr_factory.start()
            print("rekby, ydbd started")
            print("rekby, ydbd nodes", self._kikimr_factory.nodes)
            node = self._kikimr_factory.nodes[1]
            pgwire_port = node.pgwire_port
            print("rekby, pgwire port: ", pgwire_port)

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
                    "PGHOST=localhost",
                    f"PGPORT={pgwire_port}",
                    "PGDATABASE=local",
                    "PQGOSSLTESTS=0",
                    "PQSSLCERTTEST_PATH=certs",
                    f"YDB_PG_TESTFILTER={test_filter}",
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
        except BaseException as e:
            print("rekby ydbd exception: ", e)
            raise
        finally:
            print("rekby, stop ydbd")
            try:
                self._kikimr_factory.stop()
            except RuntimeError as e:
                print("rekby, stop ydbd runtime failed:", e)
                # TODO
                # handle crash ydbdb as mark in tests


    @property
    def _test_result_folder(self):
        return path.join(yatest.common.output_path(), "test-result")

def _read_tests(folder: str) -> Set[str]:
    with open(path.join(folder, "full-test-list.txt"), "rt") as f:
        all = set(line.strip() for line in f.readlines())

    with open(path.join(folder, "unit-tests.txt"), "rt") as f:
        unit = set(f.readlines())

    test_list_for_run = list(all - unit)
    test_list_for_run.sort()
    return test_list_for_run

def _read_tests_result(filepath: str) -> Dict[str, TestCase]:
    with open(filepath, "rt") as f:
        data = f.read()
    d = xmltodict.parse(data, force_list=("testcase",))
    testsuites = d["testsuites"]
    test_suite = testsuites["testsuite"]
    test_cases = test_suite["testcase"]

    res: Dict[str, TestCase] = dict()

    def get_text(test_case, field_name: str)->str:
        field_val = test_case[field_name]
        if type(field_val) == str:
            return field_val
        elif type(field_val) == dict:
            prefix=field_val.get("@message", "") + "\n"
            if prefix == "\n":
                prefix = ""
            return prefix + field_val.get("#text", "")
        raise Exception(f"Unknown field val for field '{field_name}':\n{field_val}")

    for test_case in test_cases:
        name = test_case["@classname"] + "/" + test_case["@name"]
        print("rekby-debug", test_case)
        if "failure" in test_case:
            test_state = TestState.FAILED
            log = get_text(test_case, "failure")
        elif "error" in test_case:
            test_state = TestState.FAILED
            log = get_text(test_case, "error")
        elif "skipped" in test_case:
            test_state = TestState.SKIPPED
            log = get_text(test_case, "skipped")
        else:
            test_state = TestState.PASSED
            log = ""

        res[name] = TestCase(
            name=name,
            state=test_state,
            log=log,
        )

    return res

class PgTestWrapper:
    integrations: IntegrationTests

    @classmethod
    def initialize(cls, integrations: IntegrationTests, selected_items: List[pytest.Function]):
        print("rekby wrapper initialize")
        cls.integrations = integrations
        integrations.set_selected_items(selected_items)

        print("rekby wrapper settings filter formatter")
        integrations.set_filter_formatter(cls.filter_format)
        integrations.tearup()

    def test_initialization(self):
        # for collect initializations log
        pass

    def test_pg_generated(self, testname):
        self.integrations.execute_test(testname)

    def filter_format(cls, test_names: List[str])->str:
        raise NotImplemented()
