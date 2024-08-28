import os
import shutil

from typing import Callable, Dict, List, Set, Optional, Union
from os import path
from dataclasses import dataclass
from enum import Enum

import docker
import xmltodict
import pytest

import yatest

import logging

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


_tests_for_run_in_docker: pytest.Session = []
_filter_format_function = Callable[[List[str]], str]
_filter_formatter: Optional[_filter_format_function] = None
_tests_folder: Optional[str] = None
_test_results: Optional[Dict[str, TestCase]] = None
_kikimr_factory: KiKiMR = kikimr_cluster_factory()
_integration_tests: Optional[List[str]] = None
_skip_tests: Dict[str, str] = dict() # [test name: reason]

def pytest_collection_finish(session: pytest.Session):
    global _tests_for_run_in_docker

    print("rekby set selected items: ", session.items)
    selected_tests = []
    for item in session.items:
        print(f"rekby, selected item name: '{item.name}'", )
        if item.name.startswith("test_pg_generated["):
            print("rekby selected test item:", item)
            test_name = item.callspec.id
            print(f"rekby selected test: {test_name}")
            selected_tests.append(test_name)
    selected_tests.sort()
    print("rekby: result selected tests", selected_tests)
    _tests_for_run_in_docker = list()
    for test in selected_tests:
        if test not in _skip_tests:
            _tests_for_run_in_docker.append(test)
    print("rekby, tests for run", _tests_for_run_in_docker)


def set_filter_formatter(f: _filter_format_function):
    global _filter_formatter
    _filter_formatter = f


def set_tests_folder(folder: str):
    global _tests_folder, _integration_tests, _skip_tests
    print("rekby, set_tests_folder called")
    _tests_folder = folder
    _integration_tests = _read_integration_tests(folder)
    _skip_tests = _read_skip_tests(folder)


def setup_module(module: pytest.Module):
    if len(_tests_for_run_in_docker) == 0:
        return

    global _test_results
    try:
        exchange_folder = path.join(yatest.common.output_path(), "exchange")
        os.mkdir(exchange_folder)
    except FileExistsError:
        pass

    tests_result_folder = path.join(yatest.common.output_path(), "test-result")
    shutil.rmtree(tests_result_folder, ignore_errors=True)
    os.mkdir(tests_result_folder)

    image = _docker_build(_tests_folder)

    pg_port = _run_ydb()
    env = _prepare_docker_env(pg_port, _tests_for_run_in_docker)
    _run_tests_in_docker(image, env, exchange_folder, tests_result_folder)

    test_results_file = path.join(tests_result_folder, "raw", "result.xml")
    _test_results = _read_tests_result(test_results_file)


def teardown_module(module):
    """teardown any state that was previously setup with a setup_module
    method.
    """
    _stop_ydb()


def _run_ydb() -> int:
    """
    Run YDB cluster and return pgwire port number.
    """
    _kikimr_factory.start()
    node = _kikimr_factory.nodes[1]
    print("rekby: pgwire port", node.pgwire_port)
    return node.pgwire_port


def _stop_ydb():
    _kikimr_factory.stop()


def _prepare_docker_env(pgwire_port: str, test_names: List[str]) -> List[str]:
    test_filter = _filter_formatter(test_names)
    return [
        "PGUSER=root",
        "PGPASSWORD=1234",
        "PGHOST=localhost",
        f"PGPORT={pgwire_port}",
        "PGDATABASE=/Root",
        "PQGOSSLTESTS=0",
        "PQSSLCERTTEST_PATH=certs",
        f"YDB_PG_TESTFILTER={test_filter}",
    ]


def _docker_build(folder: str) -> str:
    image_name = 'ydb-pg-test-image'

    client: docker.Client = docker.from_env()
    client.images.build(
        path=folder,
        tag=image_name,
        rm=True,
        network_mode='host',
    )
    return image_name


def _run_tests_in_docker(
        image: str,
        env: Union[List[str], Dict[str, str]],
        exchange_folder: str,
        results_folder: str,
        ):

    # TODO: run YDB with scripts/receipt and get connection port/database with runtime
    client: docker.Client = docker.from_env()

    container = client.containers.create(
        image=image,
        # command="/docker-start.bash",
        # detach=True,
        # auto_remove=True,
        environment=env,
        mounts=[
            docker.types.Mount(
                target="/exchange",
                source=exchange_folder,
                type="bind",
            ),
            docker.types.Mount(
                target="/test-result",
                source=results_folder,
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


def pytest_generate_tests(metafunc: pytest.Metafunc):
    """
    Return tests for run through pytest.
    """
    print("rekby, integration tests:", _integration_tests)
    metafunc.parametrize('testname', _integration_tests, ids=_integration_tests)


def execute_test(testname: str):
    if testname in _skip_tests:
        pytest.skip(_skip_tests[testname])

    try:
        test = _test_results[testname]
    except KeyError:
        pytest.fail("test result not found, may be the test was not runned")

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


def _read_integration_tests(folder: str) -> Set[str]:
    with open(path.join(folder, "full-test-list.txt"), "rt") as f:
        all = set(line.strip() for line in f.readlines())

    with open(path.join(folder, "unit-tests.txt"), "rt") as f:
        unit = set(f.readlines())

    test_list_for_run = list(all - unit)
    test_list_for_run.sort()
    return test_list_for_run

def _read_skip_tests(folder: str) -> Dict[str, str]:
    res = dict()
    try:
        fpath = path.join(folder, "skip-tests.txt")
        with open(fpath) as f:
            for line in f.readlines():
                if "# " in line:
                    line = line[:line.rindex("# ")]

                line = line.strip()
                if line == "":
                    continue

                res[line] = f"skipped by '{fpath}'"
    except FileNotFoundError:
        pass

    return res


def _read_tests_result(filepath: str) -> Dict[str, TestCase]:
    with open(filepath, "rt") as f:
        data = f.read()
    d = xmltodict.parse(data, force_list=("testcase",))
    testsuites = d["testsuites"]
    test_suite = testsuites["testsuite"]
    test_cases = test_suite["testcase"]

    res: Dict[str, TestCase] = dict()

    def get_text(test_case, field_name: str) -> str:
        field_val = test_case[field_name]
        if type(field_val) is str:
            return field_val
        elif type(field_val) is dict:
            prefix = field_val.get("@message", "") + "\n"
            if prefix == "\n":
                prefix = ""
            return prefix + field_val.get("#text", "")
        raise Exception(f"Unknown field val for field '{field_name}':\n{field_val}")

    for test_case in test_cases:
        class_name = test_case["@classname"]
        test_name = test_case["@name"]
        if class_name == "":
            name = test_name
        else:
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
