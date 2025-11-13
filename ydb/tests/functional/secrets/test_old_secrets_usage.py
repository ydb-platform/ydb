# -*- coding: utf-8 -*-
import logging

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

        driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root")
        self.session_pool = ydb.SessionPool(driver)
        driver.wait(5, fail_fast=True)

    def restart_cluster(self, enable_schema_secrets):
        self.config.yaml_config["feature_flags"]["enable_schema_secrets"] = enable_schema_secrets
        self.cluster.update_configurator_and_restart(self.config)

        driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root")
        driver.wait()
        self.session_pool = ydb.SessionPool(driver)

    def create_old_secret(self, secret_name, value):
        with self.session_pool.checkout() as session:
            session.execute_scheme(
                f"CREATE OBJECT {secret_name} (TYPE SECRET) WITH value='{value}';"
            )

    def alter_old_secret(self, secret_name, value):
        with self.session_pool.checkout() as session:
            session.execute_scheme(
                f"ALTER OBJECT {secret_name} (TYPE SECRET) SET value='{value}';"
            )

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

    def create_new_secret(self, secret_name, value):
        with self.session_pool.checkout() as session:
            session.execute_scheme(
                f"CREATE SECRET {secret_name} WITH (value='{value}');"
            )


def test_create_eds_with_old_secret_after_enablig_new_secrets_with_success():
    utils = Utils()
    utils.create_old_secret("OldSecret", value="")
    utils.create_eds("eds-before-restart", "OldSecret")

    utils.restart_cluster(enable_schema_secrets=True)
    utils.create_new_secret("NewSecret", value="")  # check that `enable_schema_secrets` flag is really ON

    utils.create_eds("eds-after-restart", "OldSecret")  # it's still usable
    utils.alter_old_secret("OldSecret", "NewValue")  # it's still can be altered
