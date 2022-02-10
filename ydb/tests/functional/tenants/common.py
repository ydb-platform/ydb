# -*- coding: utf-8 -*-
import os
import logging

from ydb.tests.library.harness.kikimr_http_client import HiveClient
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from ydb.tests.library.harness.util import LogLevels
import ydb


logger = logging.getLogger(__name__)


class Runtime(object):
    def __init__(self, cluster, tenant_affiliation=None, initial_count=1):
        self._cluster = cluster
        self._tenant_affiliation = tenant_affiliation
        self._initial_count = initial_count

        # use console features
        self._scheme_root_key = os.path.join(
            "/",
            cluster.domain_name,
            self._tenant_affiliation
        )

        self._allocated_slots = self._cluster.register_slots(self._scheme_root_key, self._initial_count)
        for _ in range(self._initial_count):
            self._allocated_slots.append(
                self._cluster.register_slot(
                    self._scheme_root_key
                )
            )

    @property
    def root_dir(self):
        return self._scheme_root_key

    @property
    def slots(self):
        return self._allocated_slots

    def __enter__(self):
        for slot in self._allocated_slots:
            slot.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for slot in self._allocated_slots:
            try:
                slot.stop()
            except Exception:
                logger.exception(
                    "failed to stop daemon...."
                )


class DBForStaticSlots(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            configurator=KikimrConfigGenerator(
                additional_log_configs={
                    'TX_PROXY': LogLevels.DEBUG,
                    'KQP_PROXY': LogLevels.DEBUG,
                    'KQP_WORKER': LogLevels.DEBUG,
                    'KQP_GATEWAY': LogLevels.DEBUG,
                    'GRPC_PROXY': LogLevels.TRACE,
                    'KQP_YQL': LogLevels.DEBUG,
                    'TX_DATASHARD': LogLevels.DEBUG,
                    'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
                    'TX_PROXY_SCHEME_CACHE': LogLevels.DEBUG,
                    'GRPC_SERVER': LogLevels.DEBUG,
                }
            ),
        )
        cls.cluster.start()
        cls.client = cls.cluster.client
        cls.root_dir = os.path.join("/", cls.cluster.domain_name)
        first_node = cls.cluster.nodes[1]
        cls.boot_per_node = 1
        cls.boot_batch_size = 5
        hive_cli = HiveClient(first_node.host, first_node.mon_port)
        hive_cli.set_max_scheduled_tablets(cls.boot_per_node)
        hive_cli.set_max_boot_batch_size(cls.boot_batch_size)
        cls.database_name = None

        cls.robust_retries = ydb.RetrySettings().with_fast_backoff(
            ydb.BackoffSettings(ceiling=10, slot_duration=0.05, uncertain_ratio=0.1)
        )

    @classmethod
    def teardown_class(cls):
        logger.info("teardown class")
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def setup_method(self, method=None):
        self.database_name = "/Root/users/{class_name}_{method_name}".format(
            class_name=self.__class__.__name__,
            method_name=method.__name__,
        )
        self.cluster.create_database(
            self.database_name,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            self.database_name
        )

    def teardown_method(self, method=None):
        self.cluster.remove_database(self.database_name)
        self.database_name = None


class DBWithDynamicSlot(object):
    SLOT_COUNT = 1 
 
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                additional_log_configs={
                    'CMS_TENANTS': LogLevels.TRACE,
                    'TENANT_SLOT_BROKER': LogLevels.DEBUG,
                    'TENANT_POOL': LogLevels.DEBUG,
                    'LOCAL': LogLevels.DEBUG,
                    'NODE_BROKER': LogLevels.DEBUG,
                    'TX_DATASHARD': LogLevels.DEBUG,
                    'TX_PROXY': LogLevels.DEBUG,
                    'GRPC_SERVER': LogLevels.DEBUG,
                }
            )
        )
        cls.cluster.start()

        cls.robust_retries = ydb.RetrySettings(max_retries=20).with_fast_backoff(
            ydb.BackoffSettings(ceiling=10, slot_duration=0.05, uncertain_ratio=0.1)
        )

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()
