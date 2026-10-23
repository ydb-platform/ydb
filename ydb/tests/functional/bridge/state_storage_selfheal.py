import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import requests
from google.protobuf.json_format import ParseDict

from ydb.core.protos import cms_pb2, msgbus_pb2

from test_three_piles import TestBridgeThreePiles as BridgeThreePiles
from ydb.tests.library.harness.util import LogLevels


CONFIG_NAMES = ("StateStorageConfig", "StateStorageBoardConfig", "SchemeBoardConfig")


def groups(config, name, pile_id=None):
    result = config[name]["RingGroups"]
    return [group for group in result if pile_id is None or group["BridgePileId"] == pile_id]


def replicas(group):
    return {node for ring in group["Ring"] for node in ring["Node"]}


def placement(group):
    return tuple(tuple(ring["Node"]) for ring in group["Ring"])


def is_final(config):
    return all(
        len(groups(config, name)) == 3
        and all(
            not group.get("WriteOnly", False)
            and all(not ring.get("IsDisabled", False) for ring in group["Ring"])
            for group in groups(config, name)
        )
        for name in CONFIG_NAMES
    )


class BridgeStateStorageSelfHeal:
    nodes_per_pile = 12
    rings_count = 8
    replicas_per_ring = 1
    step_seconds = 5
    relax_seconds = 30
    timeout = 300
    self_management_options = None
    selfheal_options = None
    node_location_overrides = None

    def setup_method(self):
        for name in ("self_management_options", "selfheal_options", "node_location_overrides"):
            setattr(self, name, copy.deepcopy(getattr(self, name) or {}))
        self.bridge = BridgeThreePiles()
        self.bridge.additional_log_configs = {"BS_NODE": LogLevels.DEBUG, "CMS": LogLevels.DEBUG}
        self.bridge.nodes_count = self.nodes_per_pile * len(self.bridge.pile_names)
        # As in config/test_distconf_sentinel_node_status.py, shorten Sentinel's
        # sampling limits and timers. These are test acceleration, not test cases.
        self.bridge.cms_config = {"sentinel_config": {
            "enable": True,
            "state_storage_self_heal_config": {
                "enable": True,
                "node_good_state_limit": 3,
                "node_pretty_good_state_limit": 2,
                "node_bad_state_limit": 3,
                "wait_for_config_step": self.step_seconds * 1_000_000,
                # Keep a new Sentinel request from interrupting a four-step heal.
                "relax_time": self.relax_seconds * 1_000_000,
                "pileup_replicas": True,
                **self.selfheal_options,
            },
            "default_state_limit": 2,
            "update_config_interval": 2_000_000,
            "update_state_interval": 2_000_000,
            "update_state_timeout": 1_000_000,
            "retry_update_config": 1_000_000,
        }}
        self.bridge.self_management_options = copy.deepcopy(self.self_management_options)
        self.bridge.node_location_overrides = copy.deepcopy(self.node_location_overrides)
        self.bridge.setup_method()
        try:
            self.node_piles = {
                node.node_id: pile_id
                for pile_id, pile in enumerate(self.bridge.pile_names, 1)
                for node in self.bridge.pile_nodes[pile]
            }
            self.control_node = self.bridge.pile_nodes["r3"][-1]
            self.snapshots = []
            self.observer_errors = []
            self.observer_stop = threading.Event()

            def ready():
                config = self.read_config(nodes_state=True)
                states = {entry["NodeId"]: entry["State"] for entry in config.get("NodesState", [])}
                assert len(states) == self.bridge.nodes_count, states
                assert all(state == 0 for state in states.values()), states
                self.assert_pile_locality(config)
                assert is_final(config), config
                for pile_id in range(1, 4):
                    expected = placement(groups(config, CONFIG_NAMES[0], pile_id)[0])
                    for name in CONFIG_NAMES:
                        group = groups(config, name, pile_id)[0]
                        assert len(group["Ring"]) == self.rings_count, group
                        assert all(len(ring["Node"]) == self.replicas_per_ring for ring in group["Ring"]), group
                        assert placement(group) == expected, (name, group, expected)
                return config

            self.configure_sentinel()
            self.initial = self.wait(ready, "healthy, colocated replicas on all three piles")
            self.bridge.create_data()
        except BaseException:
            self.bridge.teardown_method()
            raise

    def configure_sentinel(self):
        def protobuf_names(config):
            return {
                "".join(part.capitalize() for part in key.split("_")): (
                    protobuf_names(value) if isinstance(value, dict) else value
                )
                for key, value in config.items()
            }

        config = protobuf_names(self.bridge.cms_config)
        # CMS may become active before its local YAML subscription is applied.
        # Initialize the real Sentinel explicitly with the test configuration.
        # Restart it so existing updater timers also use the shortened intervals.
        for enabled in (False, True):
            request = msgbus_pb2.TCmsRequest()
            ParseDict(config, request.SetConfigRequest.Config)
            request.SetConfigRequest.Config.SentinelConfig.Enable = enabled
            response = self.bridge.cluster.client.send_request(request, "CmsRequest")
            assert response.SetConfigResponse.Status.Code == cms_pb2.TStatus.OK, response

    def teardown_method(self):
        self.bridge.teardown_method()

    def wait(self, action, description, timeout=None):
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        last_error = None
        while time.monotonic() < deadline:
            self.check_observer()
            try:
                return action()
            except (AssertionError, KeyError, requests.RequestException) as error:
                last_error = error
                time.sleep(0.5)
        raise AssertionError(f"Timed out waiting for {description}") from last_error

    def invoke(self, command, timeout=10):
        node = self.control_node
        response = requests.post(
            f"http://{node.host}:{node.mon_port}/actors/nodewarden?page=distconf",
            headers={"content-type": "application/json"}, json=command, timeout=timeout,
        )
        response.raise_for_status()
        result = response.json()
        # Protobuf JSON omits the default enum value (OK).
        if result.get("Status", "OK") != "OK":
            raise requests.RequestException(f"Distconf request {command} failed: {result}")
        return result

    def read_config(self, nodes_state=False, timeout=10):
        return self.invoke({"GetStateStorageConfig": {"NodesState": nodes_state}}, timeout=timeout)["StateStorageConfig"]

    def assert_pile_locality(self, config):
        for name in CONFIG_NAMES:
            by_pile = {pile_id: [] for pile_id in range(1, 4)}
            actor_ids = set()
            for group in groups(config, name):
                pile_id = group["BridgePileId"]
                assert pile_id in by_pile, (name, group)
                by_pile[pile_id].append(group)
                assert 0 < group["NToSelect"] <= len(group["Ring"]), (name, group)
                assert group["Ring"], (name, group)
                for ring_index, ring in enumerate(group["Ring"], 1):
                    assert ring["Node"], (name, group)
                    for replica_index, node_id in enumerate(ring["Node"], 1):
                        assert self.node_piles[node_id] == pile_id, (name, group, node_id)
                        offset = group.get("RingGroupActorIdOffset", 0)
                        actor_id = (
                            node_id, ring_index,
                            offset if ring.get("UseSingleNodeActorId", False) else offset + replica_index,
                        )
                        assert actor_id not in actor_ids, (name, group, actor_id)
                        actor_ids.add(actor_id)
            assert all(1 <= len(value) <= 2 for value in by_pile.values()), (name, by_pile)

    def check_observer(self):
        if self.observer_errors:
            raise self.observer_errors[0]

    @contextmanager
    def observe(self):
        self.snapshots = []
        self.observer_errors = []
        self.observer_stop.clear()

        def collect():
            while not self.observer_stop.is_set():
                try:
                    # Bound an in-flight read so teardown can stop the observer.
                    config = self.read_config(timeout=(1, 3))
                except (KeyError, requests.RequestException):
                    # A failed read is not an observed configuration. The main
                    # thread still requires every expected phase before its deadline.
                    self.observer_stop.wait(0.5)
                    continue
                except BaseException as error:
                    self.observer_errors.append(error)
                    return
                try:
                    self.assert_pile_locality(config)
                    if not self.snapshots or self.snapshots[-1][1] != config:
                        self.bridge.logger.info("State storage selfheal configuration: %s", config)
                        self.snapshots.append((time.monotonic(), config))
                except BaseException as error:
                    self.observer_errors.append(error)
                    return
                self.observer_stop.wait(0.5)

        thread = threading.Thread(target=collect, name="observe-state-storage-selfheal", daemon=True)
        thread.start()
        try:
            self.wait(lambda: self.require_snapshot(), "initial selfheal observation")
            yield
        finally:
            self.observer_stop.set()
            thread.join(15)
            assert not thread.is_alive(), "state storage observer did not stop"
            self.check_observer()

    def require_snapshot(self):
        assert self.snapshots
        return self.snapshots[-1][1]

    def choose_victims(self, pile, count, require_spares=True):
        pile_id = self.bridge.pile_names.index(pile) + 1
        candidates = set.intersection(*(replicas(groups(self.initial, name, pile_id)[0]) for name in CONFIG_NAMES))
        # Keep the pinned SDK and Bridge RPC endpoints available. Dedicated
        # failover cases exercise loss of the control/root node separately.
        candidates.discard(self.bridge.pile_nodes[pile][0].node_id)
        candidates.discard(self.control_node.node_id)
        root = self.wait(
            lambda: self.invoke({"GetStateStorageConfig": {}})["Scepter"]["NodeId"],
            "distconf root election",
        )
        candidates.discard(root)
        assert len(candidates) >= count, (pile, candidates)
        victims = sorted(candidates)[:count]
        for name in CONFIG_NAMES:
            current = replicas(groups(self.initial, name, pile_id)[0])
            option = {
                "StateStorageConfig": "state_storage_self_heal_allowed_nodes",
                "StateStorageBoardConfig": "state_storage_board_self_heal_allowed_nodes",
                "SchemeBoardConfig": "scheme_board_self_heal_allowed_nodes",
            }[name]
            allowed = self.self_management_options.get(option, [])
            spare = {
                node.node_id for node in self.bridge.pile_nodes[pile]
                if node.node_id not in current and node.is_alive()
                and (not allowed or node.node_id in allowed)
            }
            if require_spares:
                assert len(spare) >= count, (name, pile, spare)
            else:
                assert not spare, (name, pile, spare)
        return victims

    def stop_nodes(self, node_ids):
        # Stop together, so Sentinel sees two failed replicas before choosing a
        # repair strategy instead of completing two independent one-node repairs.
        with ThreadPoolExecutor(max_workers=len(node_ids)) as executor:
            list(executor.map(lambda node_id: self.bridge.cluster.nodes[node_id].stop(), node_ids))

    def wait_bad_nodes(self, victims):
        def bad():
            config = self.read_config(nodes_state=True)
            states = {entry["NodeId"]: entry["State"] for entry in config.get("NodesState", [])}
            assert all(states.get(node_id) == 4 for node_id in victims), states

        self.wait(bad, f"Sentinel reporting nodes {victims} as BAD")

    def wait_healed(self, victims):
        def healed():
            config = self.require_snapshot()
            assert is_final(config), config
            for name in CONFIG_NAMES:
                for group in groups(config, name):
                    assert not replicas(group).intersection(victims), (name, group, victims)
            return config

        return self.wait(healed, f"automatic replacement of {victims}")

    def assert_reassign(self, before, after, victims):
        assert len(victims) == 1
        victim = victims[0]
        pile_id = self.node_piles[victim]
        for name in CONFIG_NAMES:
            initial_group = groups(before, name, pile_id)[0]
            final_group = groups(after, name, pile_id)[0]
            assert initial_group.get("RingGroupActorIdOffset", 0) == final_group.get("RingGroupActorIdOffset", 0)
            assert len(initial_group["Ring"]) == len(final_group["Ring"])
            removed = replicas(initial_group) - replicas(final_group)
            added = replicas(final_group) - replicas(initial_group)
            assert removed == {victim}, (name, removed)
            assert len(added) == 1, (name, added)
            for old_ring, new_ring in zip(initial_group["Ring"], final_group["Ring"]):
                assert [node for node in old_ring["Node"] if node != victim] == [
                    node for node in new_ring["Node"] if node not in added
                ], (name, old_ring, new_ring)
            assert any(
                any(ring.get("IsDisabled", False) and victim in ring["Node"] for ring in group["Ring"])
                for _, config in self.snapshots for group in groups(config, name, pile_id)
            ), f"{name}: did not observe the disabled source ring"
            assert all(len(groups(config, name)) == 3 for _, config in self.snapshots), name
            for other in set(range(1, 4)) - {pile_id}:
                assert groups(before, name, other) == groups(after, name, other), (name, other)

    def assert_full_reconfiguration(self, before, after):
        for name in CONFIG_NAMES:
            old = {pile_id: groups(before, name, pile_id)[0] for pile_id in range(1, 4)}
            new = {pile_id: groups(after, name, pile_id)[0] for pile_id in range(1, 4)}
            for pile_id in range(1, 4):
                assert old[pile_id].get("RingGroupActorIdOffset", 0) != new[pile_id].get("RingGroupActorIdOffset", 0)
            expected_phases = [
                [("old", False), ("new", True)],
                [("old", False), ("new", False)],
                [("new", False), ("old", True)],
                [("new", False)],
            ]
            next_phase = 0
            for _, config in self.snapshots:
                if next_phase == len(expected_phases):
                    break
                matches = True
                for pile_id in range(1, 4):
                    observed = groups(config, name, pile_id)
                    expected = expected_phases[next_phase]
                    if len(observed) != len(expected):
                        matches = False
                        break
                    for group, (version, write_only) in zip(observed, expected):
                        reference = old[pile_id] if version == "old" else new[pile_id]
                        matches &= (
                            placement(group) == placement(reference)
                            and group.get("RingGroupActorIdOffset", 0) == reference.get("RingGroupActorIdOffset", 0)
                            and group.get("WriteOnly", False) == write_only
                        )
                if matches:
                    next_phase += 1
            assert next_phase == len(expected_phases), (name, next_phase, self.snapshots)

    def wait_converged(self, expected):
        def local_meta(node):
            response = requests.get(
                f"http://{node.host}:{node.mon_port}/actors/nodewarden?page=distconf&json=1", timeout=10,
            )
            response.raise_for_status()
            meta = response.json()["configs"]["effective"]
            assert meta is not None, node.node_id
            return meta

        def converged():
            actual = self.read_config()
            for name in CONFIG_NAMES:
                assert actual[name] == expected[name], (name, actual[name], expected[name])
            # GetStateStorageConfig is forwarded to the root. Compare local
            # effective fingerprints as well, so this checks propagation to every
            # live node rather than repeatedly reading the root through proxies.
            expected_meta = local_meta(self.control_node)
            for nodes in self.bridge.pile_nodes.values():
                for node in nodes:
                    if node.is_alive():
                        assert local_meta(node) == expected_meta, (node.node_id, expected_meta)

        self.wait(converged, "state storage configuration convergence")
