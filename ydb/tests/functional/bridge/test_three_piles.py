import time
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest
import ydb

from common import BridgeKiKiMRTest
from ydb.public.api.grpc.ydb_discovery_v1_pb2_grpc import DiscoveryServiceStub
from ydb.public.api.protos import ydb_discovery_pb2 as discovery
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


PRIMARY = PileState.PRIMARY
SYNC = PileState.SYNCHRONIZED
DISCONNECTED = PileState.DISCONNECTED
UNSYNC = PileState.NOT_SYNCHRONIZED


class TestBridgeThreePiles(BridgeKiKiMRTest):
    pile_names = ("r1", "r2", "r3")
    nodes_count = 24
    transition_timeout = 120
    # Repeated rejoins accumulate system-tablet history; copying it is rate-limited.
    synchronization_timeout = 300

    def setup_method(self):
        self.states = {"r1": PRIMARY, "r2": SYNC, "r3": SYNC}
        self.drivers = {}
        self.pools = {}
        self.rows = {}
        self.epoch = 0
        super().setup_method()
        try:
            assert self.nodes_count % len(self.pile_names) == 0
            assert all(len(nodes) == self.nodes_count // len(self.pile_names) for nodes in self.pile_nodes.values())
            assert self.configurator.yaml_config["erasure"] == "block-4-2"
            self.wait_states()
        except Exception:
            self.teardown_method()
            raise

    def teardown_method(self):
        try:
            for name in list(self.drivers):
                self.stop_pile_clients(name)
        finally:
            super().teardown_method()

    @property
    def primary(self):
        return next(name for name, state in self.states.items() if state == PRIMARY)

    def eventually(self, action, description, timeout=None):
        deadline = time.monotonic() + (self.transition_timeout if timeout is None else timeout)
        last_error = None
        while time.monotonic() < deadline:
            try:
                return action()
            except (AssertionError, grpc.RpcError, ydb.Error) as error:
                last_error = error
                time.sleep(0.5)
        states = {name: PileState.State.Name(state) for name, state in self.states.items()}
        raise AssertionError(f"Timed out waiting for {description}; expected states: {states}") from last_error

    def wait_states(self, timeout=None):
        def check():
            for name, state in self.states.items():
                if state != DISCONNECTED:
                    self.get_cluster_state_and_check(self.bridge_clients[name], self.states)
        self.eventually(check, "cluster state on every connected pile", timeout=timeout)

    def update(self, changes, via=None, expected_status=StatusIds.SUCCESS, quorum_piles=None):
        self.logger.info(
            "Updating bridge via %s: %s", via or self.primary,
            [(name, PileState.State.Name(state)) for name, state in changes],
        )
        client = self.bridge_clients[via or self.primary]
        updates = [PileState(pile_name=name, state=state) for name, state in changes]
        if expected_status != StatusIds.SUCCESS:
            return self.update_cluster_state(client, updates, expected_status, quorum_piles)

        attempted = False
        completed_states = {PileState.PROMOTED: PRIMARY, PileState.SUSPENDED: DISCONNECTED, UNSYNC: SYNC}

        def apply():
            nonlocal attempted
            if attempted:
                # A timed-out RPC may have committed, including an automatic state transition.
                result = self.get_cluster_state(client)
                actual = {pile.pile_name: pile.state for pile in result.pile_states}
                if all(actual[name] in (state, completed_states.get(state, state)) for name, state in changes):
                    return
            attempted = True
            response = client.update_cluster_state(updates, quorum_piles=quorum_piles)
            if response.operation.status == StatusIds.SUCCESS:
                return
            if (response.operation.status == StatusIds.INTERNAL_ERROR
                    and any(issue in str(response.operation.issues) for issue in (
                        "No quorum to start propose/commit configuration", "Hop node disconnected",
                    ))):
                self.logger.warning("Retrying bridge update after transient error: %s", response.operation.issues)
                raise AssertionError(response)
            raise RuntimeError(f"Bridge update failed: {response}")

        self.eventually(apply, f"bridge update {changes}")

    def disconnect(self, name, new_primary=None, graceful=False):
        old_primary = self.primary
        changes = [(name, PileState.SUSPENDED if graceful else DISCONNECTED)]
        if name == old_primary:
            assert new_primary is not None
            changes.append((new_primary, PRIMARY))
        # Send the request through a surviving pile, including after a physical failure.
        via = new_primary or (old_primary if name != old_primary else None)
        self.update(changes, via=via)
        self.states[name] = DISCONNECTED
        if new_primary is not None:
            self.states[new_primary] = PRIMARY
        self.wait_states()

    def rejoin(self, name, wait=True):
        self.update([(name, UNSYNC)])
        # Automatic synchronization may finish before the next state request.
        self.states[name] = SYNC
        if wait:
            self.wait_states(timeout=self.synchronization_timeout)

    def promote(self, name):
        old_primary = self.primary
        self.update([(name, PileState.PROMOTED)])
        self.states[old_primary] = SYNC
        self.states[name] = PRIMARY
        self.wait_states()

    def stop_pile_clients(self, name):
        pool = self.pools.pop(name, None)
        driver = self.drivers.pop(name, None)
        try:
            if pool is not None:
                pool.stop()
        finally:
            if driver is not None:
                self.stop_driver(driver)

    def stop_pile(self, name):
        # Stop keepalives before taking the endpoint down. A pinned driver cannot
        # reconnect once its only connection has been removed from the SDK cache.
        self.stop_pile_clients(name)
        with ThreadPoolExecutor(max_workers=len(self.pile_nodes[name])) as executor:
            list(executor.map(lambda node: node.stop(), self.pile_nodes[name]))
        assert all(not node.is_alive() for node in self.pile_nodes[name])

    def start_pile(self, name):
        with ThreadPoolExecutor(max_workers=len(self.pile_nodes[name])) as executor:
            list(executor.map(lambda node: node.start(), self.pile_nodes[name]))

    def execute(self, query, scheme=False):
        name = self.primary
        settings = ydb.BaseRequestSettings().with_timeout(10)

        def operation(session):
            if scheme:
                return session.execute_scheme(query, settings=settings)
            return session.transaction(ydb.SerializableReadWrite()).execute(
                query, commit_tx=True, settings=settings,
            )

        def attempt():
            if name not in self.drivers:
                node = self.pile_nodes[name][0]
                # Keep requests on this pile instead of discovering nodes that a later
                # transition disconnects. Discovery itself is covered separately.
                driver = ydb.Driver(ydb.DriverConfig(
                    endpoint=f"grpc://{node.endpoint}", database="/Root", disable_discovery=True,
                ))
                self.drivers[name] = driver
                self.pools[name] = ydb.SessionPool(driver, size=1)
            try:
                return self.pools[name].retry_operation_sync(
                    operation, retry_settings=ydb.RetrySettings(max_retries=0, get_session_client_timeout=5),
                )
            except (grpc.RpcError, ydb.ConnectionError, ydb.Unavailable, ydb.SessionPoolEmpty):
                # Session acquisition can hide a lost connection behind
                # SessionPoolEmpty. Retry with a new driver pinned to the same node.
                self.stop_pile_clients(name)
                raise

        return self.eventually(attempt, f"table operation through {name}")

    def create_data(self):
        self.execute(
            "CREATE TABLE `/Root/bridge_data` (id Uint64, value Utf8, PRIMARY KEY(id));",
            scheme=True,
        )
        self.write_data()

    def write_data(self):
        # Idempotent transactions allow retrying an ambiguous transport failure.
        # Every epoch adds rows, overwrites an old row and deletes another old row.
        self.epoch += 1
        new_rows = {
            self.epoch * 100 + i: f"epoch-{self.epoch}-row-{i}-" + "x" * 1024
            for i in range(16)
        }
        new_rows[0] = f"latest-{self.epoch}"
        deleted_key = (self.epoch - 1) * 100 + 1
        values = ",".join(f'({key}u, "{value}")' for key, value in new_rows.items())
        self.execute(
            f"UPSERT INTO `/Root/bridge_data` (id, value) VALUES {values};"
            f"DELETE FROM `/Root/bridge_data` WHERE id = {deleted_key}u;"
        )
        self.rows.update(new_rows)
        self.rows.pop(deleted_key, None)
        self.check_data()

    def check_data(self):
        self.logger.info("Checking %d rows through %s at epoch %d", len(self.rows), self.primary, self.epoch)
        result = self.execute("SELECT id, value FROM `/Root/bridge_data` ORDER BY id;")
        actual = {row.id: row.value for row in result[0].rows}
        assert actual == self.rows

    def keep_only(self, survivor):
        self.logger.info("Leaving %s as the only connected pile", survivor)
        if self.primary != survivor:
            self.disconnect(self.primary, new_primary=survivor)
            self.check_data()
        for name in self.pile_names:
            if name != survivor and self.states[name] != DISCONNECTED:
                self.disconnect(name)
                self.check_data()

    @pytest.mark.parametrize("target", ["r2", "r3"])
    def test_switchover(self, target):
        self.create_data()
        self.promote(target)
        self.check_data()
        self.write_data()

    def test_primary_cycle(self):
        self.create_data()
        for target in ("r2", "r3", "r1"):
            self.promote(target)
            self.check_data()
            self.write_data()

    @pytest.mark.parametrize("target", ["r2", "r3"])
    def test_primary_failure(self, target):
        self.create_data()
        self.stop_pile("r1")
        self.disconnect("r1", new_primary=target)
        self.check_data()
        self.write_data()
        self.start_pile("r1")
        self.rejoin("r1")
        self.keep_only("r1")
        self.write_data()

    @pytest.mark.parametrize("target", ["r2", "r3"])
    def test_takedown_and_rejoin(self, target):
        self.create_data()
        self.disconnect(target, graceful=True)
        self.write_data()
        self.rejoin(target)
        self.keep_only(target)
        self.write_data()

    @pytest.mark.parametrize("survivor", ["r1", "r3"], ids=["primary-survives", "primary-lost"])
    def test_two_piles_fail_together(self, survivor):
        self.create_data()
        failed = [name for name in self.pile_names if name != survivor]
        with ThreadPoolExecutor(max_workers=len(failed)) as executor:
            list(executor.map(self.stop_pile, failed))
        changes = [(name, DISCONNECTED) for name in failed]
        if survivor != self.primary:
            changes.append((survivor, PRIMARY))
        self.update(changes, via=survivor)
        self.states = {name: PRIMARY if name == survivor else DISCONNECTED for name in self.pile_names}
        self.wait_states()
        self.check_data()
        self.write_data()

    @pytest.mark.parametrize("failover", [False, True], ids=["switchover", "failover"])
    def test_switch_with_third_pile_disconnected(self, failover):
        self.create_data()
        self.disconnect("r3")
        self.write_data()
        if failover:
            self.stop_pile("r1")
            self.disconnect("r1", new_primary="r2")
        else:
            self.promote("r2")
        self.check_data()
        self.write_data()
        self.rejoin("r3")
        self.keep_only("r3")

    @pytest.mark.parametrize(
        "disconnect_order,return_order",
        [(("r2", "r3"), ("r2", "r3")), (("r1", "r2"), ("r2", "r1"))],
        ids=["survive-r1-forward-rejoin", "survive-r3-reverse-rejoin"],
    )
    @pytest.mark.parametrize("verify_index", [0, 1], ids=["verify-first", "verify-second"])
    def test_two_lagging_piles(self, disconnect_order, return_order, verify_index):
        self.create_data()
        survivor = next(name for name in self.pile_names if name not in disconnect_order)
        for name in disconnect_order:
            self.disconnect(name, new_primary=survivor if name == self.primary else None)
            self.write_data()
        for name in return_order:
            self.rejoin(name)
            self.check_data()
            self.write_data()
        # Verify each restored pile in its own run. Rejoining peers between isolation
        # checks would add another recovery cycle and change the data being verified.
        self.keep_only(return_order[verify_index])
        self.write_data()

    @pytest.mark.parametrize("verify_pile", ["r2", "r3"])
    def test_rejoin_without_waiting_for_other_pile(self, verify_pile):
        self.create_data()
        for name in ("r2", "r3"):
            self.disconnect(name)
            self.write_data()
        self.rejoin("r2", wait=False)
        self.rejoin("r3", wait=False)
        self.wait_states(timeout=self.synchronization_timeout)
        self.keep_only(verify_pile)
        self.write_data()

    def check_discovery(self):
        expected_nodes = {
            node.node_id: name for name, nodes in self.pile_nodes.items() for node in nodes
        }

        def check():
            for name, nodes in self.pile_nodes.items():
                with grpc.insecure_channel(nodes[0].endpoint) as channel:
                    response = DiscoveryServiceStub(channel).ListEndpoints(
                        discovery.ListEndpointsRequest(database="/Root"), timeout=self.bridge_rpc_timeout,
                    )
                assert response.operation.status == StatusIds.SUCCESS, response
                result = discovery.ListEndpointsResult()
                assert response.operation.result.Unpack(result)
                self.check_states(result, self.states)
                assert len(result.endpoints) == self.nodes_count, (name, result)
                assert {ep.node_id: ep.bridge_pile_name for ep in result.endpoints} == expected_nodes
        self.eventually(check, "discovery on all three piles")

    def test_discovery(self):
        self.check_discovery()
        for target in ("r2", "r3"):
            self.promote(target)
            self.check_discovery()

    def assert_rejected(self, changes, status, issue, quorum_piles=None):
        before = self.get_cluster_state(self.bridge_clients[self.primary])
        response = self.update(changes, expected_status=status, quorum_piles=quorum_piles)
        assert issue in str(response.operation.issues), response
        for name, state in self.states.items():
            if state != DISCONNECTED:
                after = self.get_cluster_state_and_check(self.bridge_clients[name], self.states)
                assert after.generation == before.generation

    @pytest.mark.parametrize(
        "changes,status,issue",
        [
            ([], StatusIds.BAD_REQUEST, "updates size is not >= 1"),
            ([("r2", PRIMARY), ("r3", PRIMARY)], StatusIds.BAD_REQUEST, "multiple primary piles"),
            ([("r1", SYNC)], StatusIds.BAD_REQUEST, "one primary pile"),
            ([("r2", SYNC), ("r2", PRIMARY)], StatusIds.BAD_REQUEST, "duplicate update"),
            ([("r4", PRIMARY)], StatusIds.BAD_REQUEST, "Invalid pile name"),
            ([("r2", PileState.PROMOTED), ("r3", PileState.PROMOTED)],
             StatusIds.BAD_REQUEST, "multiple promoted piles"),
        ],
        ids=["empty", "two-primary", "no-primary", "duplicate", "unknown-pile", "two-promoted"],
    )
    def test_invalid_updates(self, changes, status, issue):
        self.assert_rejected(changes, status, issue)

    def test_invalid_quorum(self):
        self.assert_rejected(
            [("r3", PileState.PROMOTED)], StatusIds.BAD_REQUEST, "Unknown pile name in quorum",
            quorum_piles=["r1", "r4"],
        )

    def test_reject_two_rejoins_in_one_request(self):
        self.create_data()
        self.disconnect("r2")
        self.write_data()
        self.disconnect("r3")
        self.write_data()
        self.assert_rejected(
            [("r2", UNSYNC), ("r3", UNSYNC)], StatusIds.INTERNAL_ERROR, "Too many state changes",
        )
        self.check_data()
        for name in ("r2", "r3"):
            self.rejoin(name)
        self.keep_only("r3")
