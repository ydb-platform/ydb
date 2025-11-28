import logging
import string
import random

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

logger = logging.getLogger(__name__)


class Utils:
    def __init__(self, enable_tiny_disks):
        self.config = KikimrConfigGenerator(
            erasure=Erasure.BLOCK_4_2,
            use_in_memory_pdisks=False
        )
        self.config.yaml_config["feature_flags"]["enable_tiny_disks"] = enable_tiny_disks

        self.cluster = KiKiMR(self.config)
        self.cluster.start()

        driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root")
        self.session_pool = ydb.SessionPool(driver)
        driver.wait(5, fail_fast=True)

    def restart_cluster(self, enable_tiny_disks):
        self.config.yaml_config["feature_flags"]["enable_tiny_disks"] = enable_tiny_disks
        self.cluster.update_configurator_and_restart(self.config)

        driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root")
        driver.wait(20)
        self.session_pool = ydb.SessionPool(driver)

    def create_table(self, table_name):
        with self.session_pool.checkout() as session:
            session.execute_scheme(
                f"create table `{table_name}` (k Uint32, v Utf8, primary key (k));"
            )

    def write_data(self, table_name, count, size):
        with self.session_pool.checkout() as session:
            prepared = session.prepare(f"""
                declare $key as Uint32;
                declare $value as Utf8;
                upsert into `{table_name}` (k, v) VALUES ($key, $value);
                """)

            for _ in range(count):
                key = random.randint(1, 2**30)
                value = ''.join(random.choice(string.ascii_lowercase) for _ in range(size))
                session.transaction().execute(
                    prepared, {'$key': key, '$value': value},
                    commit_tx=True
                )

    def read_data(self, table_name):
        with self.session_pool.checkout() as session:
            session.transaction().execute(
                f"select k, v from `{table_name}`;"
            )


class TestTinyVDisks:
    def test_disabled(self):
        utils = Utils(False)
        utils.create_table("table")
        utils.write_data("table", 1, 4194304)
        utils.restart_cluster(False)
        utils.read_data("table")

    def test_enabled(self):
        utils = Utils(True)
        utils.create_table("table")
        utils.write_data("table", 1, 4194304)
        utils.restart_cluster(True)
        utils.read_data("table")

    def test_disabled_enabled(self):
        utils = Utils(False)
        utils.create_table("table")
        utils.write_data("table", 1, 4194304)
        utils.restart_cluster(True)
        utils.read_data("table")

    def test_enabled_disabled(self):
        utils = Utils(enable_tiny_disks=True)
        utils.create_table("table")
        utils.write_data("table", 1, 4194304)
        utils.restart_cluster(False)
        utils.read_data("table")
