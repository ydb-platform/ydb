from datetime import datetime
from typing import Sequence
import time

import yatest.common

from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.docker_compose import DockerComposeHelper

LOGGER = make_logger(__name__)


class OneTimeWaiter:
    __launched: bool = False

    def __init__(
        self,
        docker_compose_file_path: str,
        data_source_kind: EGenericDataSourceKind,
        expected_tables: Sequence[str],
    ):
        docker_compose_file_abs_path = yatest.common.source_path(docker_compose_file_path)
        self.docker_compose_helper = DockerComposeHelper(docker_compose_yml_path=docker_compose_file_abs_path)
        self.expected_tables = set(expected_tables)
        self.data_source_kind = data_source_kind

    def wait(self):
        if self.__launched:
            return

        # This should be enough for tables to initialize
        start = datetime.now()

        timeout = 600
        while (datetime.now() - start).total_seconds() < timeout:
            self.actual_tables = set(self.docker_compose_helper.list_tables(self.data_source_kind))

            # check if all the required tables have been created
            if self.expected_tables <= self.actual_tables:
                self.__launched = True
                return

            LOGGER.warning(f"Not enough tables: expected={self.expected_tables}, actual={self.actual_tables}")
            time.sleep(5)

        raise ValueError(
            f"Datasource failed to initialize in {timeout} seconds, latest table set: {self.actual_tables}"
        )
