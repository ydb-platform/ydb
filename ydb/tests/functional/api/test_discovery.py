# -*- coding: utf-8 -*-
import abc
import six
import time
import logging

from hamcrest import assert_that, is_, not_

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common import types
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.public.api.grpc.ydb_discovery_v1_pb2_grpc import DiscoveryServiceStub
from ydb.public.api.protos import ydb_discovery_pb2 as discovery
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import grpc


logger = logging.getLogger(__name__)


def wait_until_discovery_includes_all_slot_ports(
    cluster,
    resolver,
    timeout_seconds=30,
    step_seconds=0.5,
    message=None,
):
    """Wait until ListEndpoints returns every tenant slot gRPC port (handles board publish lag)."""
    required = {slot.grpc_port for slot in cluster.slots.values()}
    last_observed = {"ports": None, "debug_details": None}

    def capture_debug_details():
        debug_details_getter = getattr(resolver, "debug_details", None)
        if not callable(debug_details_getter):
            return
        try:
            last_observed["debug_details"] = debug_details_getter()
        except Exception as e:
            last_observed["debug_details"] = "debug_details() failed: %r" % (e,)

    def predicate():
        try:
            resolved = resolver.resolve()
            if resolved is None:
                last_observed["ports"] = None
                return False

            ports = sorted(endpoint.port for endpoint in resolved.endpoints)
            last_observed["ports"] = ports
            return required <= set(ports)
        finally:
            # Capture debug_details after resolve() so the failure message
            # reflects the most recent attempt (resolve() itself appends details).
            capture_debug_details()

    assert_that(
        wait_for(predicate, timeout_seconds=timeout_seconds, step_seconds=step_seconds),
        is_(True),
        message
        or (
            "ListEndpoints should include every tenant slot port; "
            "expected %s, last resolved %s, resolver details %r"
            % (sorted(required), last_observed["ports"], last_observed["debug_details"])
        ),
    )


class TestDiscoveryExtEndpoint(object):
    @classmethod
    def setup_class(cls):
        conf = KikimrConfigGenerator()
        cls.ext_port_1 = conf.port_allocator.get_node_port_allocator(0).ext_port
        cls.ext_port_2 = conf.port_allocator.get_node_port_allocator(1).ext_port
        conf.clone_grpc_as_ext_endpoint(cls.ext_port_1, "extserv1")
        conf.clone_grpc_as_ext_endpoint(cls.ext_port_2, "extserv2")
        cls.cluster = KiKiMR(
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

        wait_until_discovery_includes_all_slot_ports(
            self.cluster,
            resolver,
            message="ListEndpoints via default gRPC should include every tenant slot port; missing after wait",
        )

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
        cls.cluster = KiKiMR()
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

        wait_until_discovery_includes_all_slot_ports(
            self.cluster,
            resolver,
            message="ListEndpoints should include every tenant slot before fault injection",
        )

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

        last_recovery_error = {"exception": None}

        def prepare_select_one_succeeds():
            session = None
            try:
                session = driver.table_client.session().create()
                session.prepare('select 1')
                return True
            except Exception as e:
                last_recovery_error["exception"] = e
                return False
            finally:
                if session is not None:
                    try:
                        session.delete()
                    except Exception:
                        pass

        assert_that(
            wait_for(prepare_select_one_succeeds, timeout_seconds=120, step_seconds=1.0),
            is_(True),
            "Database did not recover after fault extraction (prepare select 1); last error: %r"
            % (last_recovery_error["exception"],),
        )

        # ensure zero errors once healthy
        for _ in range(9):
            session = driver.table_client.session().create()
            try:
                session.prepare('select 1')
            finally:
                try:
                    session.delete()
                except Exception:
                    pass


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
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=types.Erasure.MIRROR_3_DC, use_in_memory_pdisks=False))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_mirror3dc_discovery_logic(self):
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), '/Root', use_all_nodes=False)
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


class TestLoadFactor(object):
    """Test that load_factor is returned and updated in ListEndpoints"""

    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR()
        cls.cluster.start()
        cls.database_name = '/Root'
        cls.logger = logger.getChild(cls.__name__)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def list_endpoints(self, channel):
        stub = DiscoveryServiceStub(channel)
        request = discovery.ListEndpointsRequest(database="/Root")
        response = stub.ListEndpoints(request)

        assert_that(response.operation.status, is_(StatusIds.SUCCESS))

        result = discovery.ListEndpointsResult()
        response.operation.result.Unpack(result)

        return result

    def test_load_factor_updates(self):
        """Test that load_factor changes over time"""
        import random
        
        endpoint = "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].grpc_port)
        channel = grpc.insecure_channel(endpoint)

        driver_config = ydb.DriverConfig(endpoint, self.database_name)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        try:
            pool = ydb.SessionPool(driver)

            result1 = self.list_endpoints(channel)
            assert_that(len(result1.endpoints) > 0, "Expected at least one endpoint")

            initial_load_factors = {}
            for ep in result1.endpoints:
                key = "%s:%d" % (ep.address, ep.port)
                initial_load_factors[key] = ep.load_factor
                self.logger.info("Initial load_factor for %s: %f", key, ep.load_factor)

            start_time = time.time()
            while time.time() - start_time < 20:
                try:
                    random_numbers = [random.randint(0, int(1e3)) for _ in range(int(1e3))]
                    values_str = ', '.join('({})'.format(n) for n in random_numbers)

                    def sort_query(session):
                        session.transaction().execute(
                            'SELECT value FROM (VALUES {}) AS t(value) ORDER BY value'.format(values_str),
                            commit_tx=True
                        )

                    pool.retry_operation_sync(sort_query)
                except Exception as e:
                    self.logger.debug("Load generation error: %s", e)

            result2 = self.list_endpoints(channel)
            assert_that(len(result2.endpoints) > 0, "Expected at least one endpoint")

            load_factor_has_changed = False
            for ep in result2.endpoints:
                key = "%s:%d" % (ep.address, ep.port)
                self.logger.info("Updated load_factor for %s: %f", key, ep.load_factor)
                assert_that(
                    ep.load_factor > 0.0,
                    "Expected load_factor > 0, got %f" % ep.load_factor
                )

                if key in initial_load_factors:
                    delta = ep.load_factor - initial_load_factors[key]
                    self.logger.info(
                        "Load factor change for %s: %f -> %f (delta: %f)",
                        key,
                        initial_load_factors[key],
                        ep.load_factor,
                        delta
                    )

                    if abs(delta) > 0:
                        load_factor_has_changed = True

            assert_that(
                load_factor_has_changed,
                "Expected at least one endpoint's load_factor to change under load"
            )

        finally:
            driver.stop()
            channel.close()
