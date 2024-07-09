from typing import List, Set, Optional
import os
from os import path

import docker
import pytest

import yatest


class IntegrationTests:
    _folder: str
    _image_name: str
    _all_tests: Set[str]
    _selected_test: Set[str]
    _docker_executed: bool

    def __init__(self, folder: str, image_name: str = 'ydb-pg-test-image'):
        self._folder = folder
        self._image_name = image_name

        self._all_tests = _read_tests(folder)
        self._selected_test = set(self._all_tests)

        self._docker_executed = False

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
        self._run_tests_in_docker(testname)

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
            test_result_folder=path.join(yatest.common.output_path(), "test-result")
            os.mkdir(test_result_folder)
        except FileExistsError:
            pass

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
                    source=test_result_folder,
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

def _read_tests(folder: str) -> Set[str]:
    with open(path.join(folder, "full-test-list.txt"), "rt") as f:
        all = set(line.strip() for line in f.readlines())

    with open(path.join(folder, "unit-tests.txt"), "rt") as f:
        unit = set(f.readlines())

    test_list_for_run = all - unit
    return test_list_for_run
