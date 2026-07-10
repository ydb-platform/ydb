# -*- coding: utf-8 -*-
import logging

import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class Utils:
    def __init__(self):
        self.config = KikimrConfigGenerator(use_in_memory_pdisks=False)
        self.config.yaml_config["feature_flags"]["enable_external_data_sources"] = True

        self.cluster = KiKiMR(self.config)
        self.cluster.start()

        self.driver = None
        self.session_pool = None
        self._start_client()

    def _start_client(self):
        self.driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root")
        self.driver.wait(5, fail_fast=True)
        self.session_pool = ydb.SessionPool(self.driver)

    def _stop_client(self):
        if self.session_pool is not None:
            self.session_pool.stop()
            self.session_pool = None
        if self.driver is not None:
            self.driver.stop()
            self.driver = None

    def stop(self):
        self._stop_client()
        self.cluster.stop()

    def restart_cluster(self, disable_old_secret_creation, enable_schema_secrets=True):
        self._stop_client()
        self.config.yaml_config["feature_flags"]["disable_old_secret_creation"] = disable_old_secret_creation
        self.config.yaml_config["feature_flags"]["enable_schema_secrets"] = enable_schema_secrets
        self.cluster.update_configurator_and_restart(self.config)
        self._start_client()

    def create_old_secret(self, secret_name, value):
        with self.session_pool.checkout() as session:
            session.execute_scheme(f"CREATE OBJECT {secret_name} (TYPE SECRET) WITH value='{value}';")

    def alter_old_secret(self, secret_name, value):
        with self.session_pool.checkout() as session:
            session.execute_scheme(f"ALTER OBJECT {secret_name} (TYPE SECRET) SET value='{value}';")

    def create_eds(self, eds_name, secret_name):
        with self.session_pool.checkout() as session:
            query = f"""
                CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_ID="mysa",
                    SERVICE_ACCOUNT_SECRET_NAME="{secret_name}"
                );"""
            session.execute_scheme(query)

    def create_schema_secret(self, secret_name, value):
        with self.session_pool.checkout() as session:
            session.execute_scheme(f"CREATE SECRET {secret_name} WITH (value='{value}');")


@pytest.fixture
def old_secrets_utils():
    utils = Utils()
    yield utils
    utils.stop()


def test_create_eds_with_old_secret_after_disabling_old_secret_creation(old_secrets_utils):
    # can create old secrets by default
    old_secrets_utils.create_old_secret("OldSecret", value="")

    # can use old secrets by default
    old_secrets_utils.create_eds("eds-before-restart", "OldSecret")

    old_secrets_utils.restart_cluster(disable_old_secret_creation=True)

    # can create schema secrets with old secrets disabled
    old_secrets_utils.create_schema_secret("NewSecret", value="")

    # can use old secrets with old secrets disabled
    old_secrets_utils.create_eds("eds-after-restart", "OldSecret")

    # can alter old secrets with old secrets disabled
    old_secrets_utils.alter_old_secret("OldSecret", "NewValue")

    # can not create old secrets with old secrets disabled
    with pytest.raises(Exception) as exc_info:
        old_secrets_utils.create_old_secret("NewOldSecret", value="")
    assert "Old secrets creation syntax is disabled now. Please use the new one" in str(exc_info.value)
