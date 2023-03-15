# -*- coding: utf-8 -*-
import logging
import random

import time
import threading
import yatest.common
from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.msgbus_types import EDriveStatus


logger = logging.getLogger(__name__)


def write_file(f, data):
    with open(f, 'w') as w:
        w.write(data)


def make_key_data_for(path):
    return 'Keys { ContainerPath: "%s" Pin: "" Id: "fake-secret" Version: 1 } ' % path


def write_encryption_key_for(database):
    d_p = database.replace('/', '_')
    ft = yatest.common.output_path(d_p + 'key.txt')
    pt = yatest.common.output_path(d_p + 'fake-secret.txt')
    write_file(pt, "fake-secret-data" + database)
    write_file(ft, make_key_data_for(pt))
    return ft


def create_sample_table(pool):
    def callee(s):
        s.execute_scheme(
            '--!syntax_v1\n create table db1 (key uint64, value utf8, primary key(key)) WITH ( UNIFORM_PARTITIONS = 64 ); '
        )

    pool.retry_operation_sync(callee)


def simple_write_data(pool, idx):

    def callee(s):
        s.transaction().execute(
            s.prepare('declare $key as Uint64;\n declare $value as Utf8;\n upsert into db1 (key, value) values ($key, $value); \n'),
            parameters={'$key': idx, '$value': str(idx) * 20},
            commit_tx=True,
        )

    try:
        pool.retry_operation_sync(callee, retry_settings=ydb.RetrySettings(max_retries=2))
    except Exception:
        logger.exception("Error executing transaction")


def simple_write(pool):
    for i in range(10000):
        simple_write_data(pool, random.randint(0, (1 << 64) - 1))


class TestEncryption(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            configurator=KikimrConfigGenerator(
                use_in_memory_pdisks=True,
                dynamic_pdisks=[{'user_kind': 0}],
                erasure=Erasure.BLOCK_4_2,
            )
        )
        cls.cluster.start()
        cls.discovery_endpoint = "%s:%s" % (cls.cluster.nodes[1].hostname, cls.cluster.nodes[1].grpc_port)

    def test_simple_encryption(self):
        databases = []
        drivers = []
        pools = []
        threads = []
        for idx in range(2):
            db_name = '/Root/test_simple_encryption_%d' % idx
            databases.append(db_name)
            self.cluster.create_database(
                db_name,
                storage_pool_units_count={
                    'hdde': 1
                }
            )

        for idx in range(2):
            db_name = '/Root/test_simple_encryption_%d' % idx
            self.cluster.register_and_start_slots(
                db_name,
                count=2,
                encryption_key=write_encryption_key_for(
                    '/Root/test_simple_encryption_%d' % idx
                )
            )

        for idx in range(2):
            db_name = '/Root/test_simple_encryption_%d' % idx
            self.cluster.wait_tenant_up(db_name)
            drivers.append(ydb.Driver(ydb.DriverConfig(self.discovery_endpoint, db_name)))
            pools.append(ydb.SessionPool(drivers[idx]))

        time.sleep(3)

        for pool in pools:
            create_sample_table(pool)

        for pool in pools:
            threads.append(
                threading.Thread(
                    target=lambda: simple_write(pool),
                )
            )

        for thread in threads:
            thread.start()

        for idx in (1, 2):
            node = self.cluster.nodes[idx]
            drives = self.cluster.client.read_drive_status(node.host, node.ic_port).BlobStorageConfigResponse
            logger.info("%s", str(drives))
            for status in drives.Status:
                for drive in status.DriveStatus:
                    logger.info("Set broken %s %s %s", node.host, node.ic_port, drive.Path)
                    for status in (EDriveStatus.BROKEN, EDriveStatus.ACTIVE):
                        resp = self.cluster.client.update_drive_status(node.host, node.ic_port, drive.Path, status)
                        logger.info("Set broken resp %s", str(resp))

            for slot in self.cluster.slots.values():
                slot.stop()
                time.sleep(1)
                slot.start()

        for thread in threads:
            thread.join()

        for pool in pools:
            pool.stop()

        for driver in drivers:
            driver.stop()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()
