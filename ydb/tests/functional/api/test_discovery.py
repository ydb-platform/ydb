# -*- coding: utf-8 -*-
import abc
import six
import time
import logging

from hamcrest import assert_that, is_, not_

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common import types
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)


class TestDiscoveryExtEndpoint(object):
    @classmethod
    def setup_class(cls):
        conf = KikimrConfigGenerator()
        cls.ext_port_1 = conf.port_allocator.get_node_port_allocator(0).ext_port
        cls.ext_port_2 = conf.port_allocator.get_node_port_allocator(1).ext_port
        conf.clone_grpc_as_ext_endpoint(cls.ext_port_1, "extserv1")
        conf.clone_grpc_as_ext_endpoint(cls.ext_port_2, "extserv2")
        cls.cluster = kikimr_cluster_factory(
            configurator=conf
        )
        cls.cluster.start()
        cls.database_name = '/Root/database'
        cls.logger = logger.getChild(cls.__name__)
        cls.cluster.create_database(
            cls.database_name,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        cls.cluster.register_and_start_slots(cls.database_name, count=2)
        cls.cluster.wait_tenant_up(cls.database_name)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_scenario(self):
        ext_port_1 = TestDiscoveryExtEndpoint.ext_port_1
        ext_port_2 = TestDiscoveryExtEndpoint.ext_port_2
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        # Discovery has been performed using default endpoint
        # but ext endpoint marked with label
        # such ext endpoint should not present in discovery
        assert_that(ext_port_1 not in endpoint_ports)
        assert_that(ext_port_2 not in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port in endpoint_ports)
            assert_that(slot.grpc_port != ext_port_1)
            assert_that(slot.grpc_port != ext_port_2)

        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, ext_port_1), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        # Discovery has been performed using external endpoint with label
        # only endpoint with such label expected
        assert_that(ext_port_1 in endpoint_ports)
        assert_that(ext_port_2 not in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port not in endpoint_ports)

        # Just check again to cover discovery cache issue
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, ext_port_1), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        assert_that(ext_port_1 in endpoint_ports)
        assert_that(ext_port_2 not in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port not in endpoint_ports)

        # Repeat using other ext endpoint
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, ext_port_2), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        assert_that(ext_port_1 not in endpoint_ports)
        assert_that(ext_port_2 in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port not in endpoint_ports)


@six.add_metaclass(abc.ABCMeta)
class AbstractTestDiscoveryFaultInjection(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.database_name = '/Root/database'
        cls.logger = logger.getChild(cls.__name__)
        cls.cluster.create_database(
            cls.database_name,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        cls.cluster.register_and_start_slots(cls.database_name, count=2)
        cls.cluster.wait_tenant_up(cls.database_name)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    @abc.abstractproperty
    def fault_injection_name(self):
        pass

    @abc.abstractmethod
    def inject_fault(self, slot):
        pass

    @abc.abstractmethod
    def extract_fault(self, slot):
        pass

    def test_scenario(self):
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        initial_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        initial_ports = initial_ports[:1]

        for slot in self.cluster.slots.values():
            if slot.grpc_port in initial_ports:
                self.inject_fault(slot)
                self.logger.info(
                    "Injected fault, slot with gRPC port: %s" % slot.grpc_port
                )

        for _ in range(10):
            try:
                session = driver.table_client.session().create()
            except Exception:
                pass

        self.logger.info("Waiting for slot to move")
        moved = False
        while not moved:
            resolv = resolver.resolve()

            if resolv is None:
                time.sleep(3)
                continue

            for endpoint in resolv.endpoints:
                if endpoint.port not in initial_ports:
                    moved = True

            if not moved:
                self.logger.debug("No additional endpoints for database")
                time.sleep(3)

        driver._discovery_thread.execute_discovery()
        driver._discovery_thread.execute_discovery()
        for slot in self.cluster.slots.values():
            if slot.grpc_port in initial_ports:
                self.extract_fault(slot)
                self.logger.info("Extracted fault, slot with gRPC port: %s" % slot.grpc_port)

        # monkey waiting until gRPC machinery will complete recovery
        time.sleep(3)

        # ensure zero errors by internal error
        for _ in range(10):
            session = driver.table_client.session().create()
            session.prepare(
                'select 1')


class TestDiscoveryFaultInjectionSlotStop(AbstractTestDiscoveryFaultInjection):
    def inject_fault(self, slot):
        slot.stop()

    def extract_fault(self, slot):
        slot.start()

    @property
    def fault_injection_name(self):
        return 'slot_stop'


class TestMirror3DCDiscovery(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(erasure=types.Erasure.MIRROR_3_DC, use_in_memory_pdisks=False))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_mirror3dc_discovery_logic(self):
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), '/Root')
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        while driver._store.size < 9:
            driver._discovery_thread.execute_discovery()
            time.sleep(1)

        def ensure_locality(step_name):
            self_location_ports = [self.cluster.nodes[idx].grpc_port for idx in range(1, 10, 3)]
            for iteration_id in range(100):
                connection = driver._store.get()

                self_location = False
                for port in self_location_ports:
                    if str(port) in connection.endpoint:
                        self_location = True

                assert_that(
                    self_location,
                    is_(True),
                    "%s, iteration id %d: expected endpoint with ports %s, but actual is %s" % (
                        step_name,
                        iteration_id,
                        str(self_location_ports),
                        connection.endpoint
                    )
                )

        ensure_locality("Initial discovery")

        for idx in range(1, 10, 3):
            self.cluster.nodes[idx].stop()

        for _ in range(6):
            try:
                driver.table_client.session().create()
            except Exception:
                continue

        for _ in range(100):
            connection = driver._store.get()

            assert_that(
                connection,
                is_(
                    not_(
                        None
                    )
                )
            )

        for idx in range(1, 10, 3):
            self.cluster.nodes[idx].start()

        while driver._store.size < 9:
            driver._discovery_thread.execute_discovery()
            time.sleep(1)

        ensure_locality("Test finish")
