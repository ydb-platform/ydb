# -*- coding: utf-8 -*-
import abc
from contextlib import ExitStack
import six
import time
import logging

import grpc
import pytest
import yatest.common
from hamcrest import assert_that, is_, not_

from ydb.public.api.grpc.draft.ydb_database_management_v1_pb2_grpc import DatabaseManagementServiceStub
from ydb.public.api.grpc.ydb_discovery_v1_pb2_grpc import DiscoveryServiceStub
from ydb.public.api.protos import ydb_discovery_pb2 as discovery
from ydb.public.api.protos.draft import ydb_database_management_pb2 as database_management
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common import types
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.oss.ydb_sdk_import import ydb


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


def _discovery_snapshot(reply):
    assert reply.operation.status == StatusIds.SUCCESS
    result = discovery.ListEndpointsResult()
    assert reply.operation.result.Unpack(result)
    return result


@pytest.fixture(scope="module", params=[False, True])
def discovery_cluster(request):
    enabled = request.param
    config = KikimrConfigGenerator(
        nodes=2, erasure=types.Erasure.NONE, default_clusteradmin="root@builtin",
        extra_grpc_services=["database_management"],
        extra_feature_flags=["enable_discovery_long_polling"] if enabled else [],
    )
    cluster = KiKiMR(config)
    cluster.start()
    try:
        yield cluster, enabled
    finally:
        cluster.stop()


def test_discovery_long_poll_hash(discovery_cluster):
    cluster, enabled = discovery_cluster
    with ExitStack() as resources:
        ports = {n.grpc_port for n in cluster.nodes.values()}
        rpcs = [DiscoveryServiceStub(resources.enter_context(grpc.insecure_channel(n.endpoint))).ListEndpoints
                for n in cluster.nodes.values()]

        def request(snapshot_hash=b""):
            return discovery.ListEndpointsRequest(database="/Root", snapshot_hash=snapshot_hash)

        def snapshot_hash(rpc, token=b""):
            result = _discovery_snapshot(rpc(request(token), timeout=5))
            assert ports <= {ep.port for ep in result.endpoints}
            return result.snapshot_hash

        def initial_hashes():
            try:
                return [snapshot_hash(rpc) for rpc in rpcs]
            except (grpc.RpcError, AssertionError):
                return None

        hashes = yatest.common.wait_for(initial_hashes, timeout=20, sleep_time=0.2)
        for rpc, current, peer in zip(rpcs, hashes, reversed(hashes)):
            assert bool(current) == enabled
            assert snapshot_hash(rpc, b"stale") == current
            if peer != current:
                assert snapshot_hash(rpc, peer) == current
        if not enabled:
            return

        polls = [rpc.future(request(current), timeout=25) for rpc, current in zip(rpcs, hashes)]
        with pytest.raises(grpc.RpcError) as expired:
            rpcs[0](request(hashes[0]), timeout=1)
        assert expired.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        for poll in polls:
            assert poll.cancel()


def test_trigger_rediscovery(discovery_cluster):
    cluster, enabled = discovery_cluster
    admin = "root@builtin"
    with ExitStack() as resources:
        channels = [resources.enter_context(grpc.insecure_channel(n.endpoint)) for n in cluster.nodes.values()]
        rpcs = [DiscoveryServiceStub(channel).ListEndpoints for channel in channels]

        management = DatabaseManagementServiceStub(channels[0])

        def reset(database, token=admin):
            metadata = [("x-ydb-auth-ticket", token)]
            if database is not None:
                metadata.append(("x-ydb-database", database))
            return management.TriggerRediscovery(
                database_management.TriggerRediscoveryRequest(), metadata=metadata, timeout=10, wait_for_ready=True).operation

        if not enabled:
            assert reset("/Root").status == StatusIds.UNSUPPORTED
            return

        assert reset("/Root").status == StatusIds.SUCCESS
        shared = cluster.create_hostel_database("/Root/shared", {"hdd": 1}, token=admin)
        slot, = cluster.register_and_start_slots(shared)
        cluster.wait_tenant_up(shared, token=admin)
        first, second = "/Root/first", "/Root/second"
        owner = "first-owner@builtin"
        for database, permissions in (
            (first, ydb.ModifyPermissionsSettings().change_owner(owner)),
            (second, ydb.ModifyPermissionsSettings().grant_permissions(owner, ["ydb.database.connect"])),
        ):
            cluster.create_serverless_database(database, hostel_db=shared, token=admin)
            with ydb.Driver(ydb.DriverConfig(slot.endpoint, database, auth_token=admin)) as driver:
                driver.wait(timeout=10)
                driver.scheme_client.modify_permissions(database, permissions)

        def snapshot(rpc, database):
            return _discovery_snapshot(rpc(discovery.ListEndpointsRequest(database=database), timeout=5))

        def boards_ready():
            try:
                return all({ep.port for ep in snapshot(rpc, db).endpoints} == {slot.grpc_port}
                           for db in (first, second) for rpc in rpcs)
            except (grpc.RpcError, AssertionError):
                return False

        assert wait_for(boards_ready, timeout_seconds=20)

        def watch(db):
            hashes = [snapshot(rpc, db).snapshot_hash for rpc in rpcs]
            assert all(hashes)
            calls = [rpc.future(discovery.ListEndpointsRequest(database=db, snapshot_hash=value), timeout=45)
                     for rpc, value in zip(rpcs, hashes)]
            return hashes, calls

        def still_waiting(calls):
            for call in calls:
                with pytest.raises(grpc.FutureTimeoutError):
                    call.result(timeout=0.5)

        def trigger(db, caller, calls):
            operation = reset(db, caller)
            assert operation.ready and operation.status == StatusIds.SUCCESS, operation.issues
            for call in calls:
                reply = call.result(timeout=10).operation
                assert reply.ready and reply.status == StatusIds.ABORTED and not reply.HasField("result")

        first_hashes, first_calls = watch(first)
        _, second_calls = watch(second)
        for db in (None, ""):
            assert reset(db).status == StatusIds.BAD_REQUEST
        assert reset(second, owner).status == StatusIds.UNAUTHORIZED
        still_waiting(first_calls + second_calls)
        trigger(first, owner, first_calls)

        fresh_hashes, fresh_calls = watch(first)
        assert fresh_hashes == first_hashes
        still_waiting(second_calls + fresh_calls)
        trigger(second, admin, second_calls)
        still_waiting(fresh_calls)
        trigger(first, owner, fresh_calls)


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
        cls.cluster = KiKiMR(configurator=KikimrConfigGenerator(extra_feature_flags=["enable_discovery_long_polling"]))
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
        with grpc.insecure_channel(self.cluster.nodes[1].endpoint) as channel:
            list_endpoints = DiscoveryServiceStub(channel).ListEndpoints
            request = discovery.ListEndpointsRequest(database=self.database_name)
            request.snapshot_hash = _discovery_snapshot(list_endpoints(request, timeout=5)).snapshot_hash
            assert request.snapshot_hash
            pending = [list_endpoints.future(request, timeout=25) for _ in range(2)]
            with pytest.raises(grpc.FutureTimeoutError):
                pending[0].result(timeout=1)
            slot.stop()
            expected = {s.grpc_port for s in self.cluster.slots.values()} - {slot.grpc_port}
            replies = [call.result() for call in pending]
            replies.append(list_endpoints(request, timeout=5))  # Same hash, requested after the board changed.
            for reply in replies:
                result = _discovery_snapshot(reply)
                assert {ep.port for ep in result.endpoints} == expected
                assert result.snapshot_hash != request.snapshot_hash

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
