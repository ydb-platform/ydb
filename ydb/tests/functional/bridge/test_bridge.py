# -*- coding: utf-8 -*-
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, call

import grpc
import pytest
import ydb

from common import BridgeKiKiMRTest

from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness import daemon


@pytest.mark.parametrize("first_status", [None, StatusIds.UNAVAILABLE, StatusIds.INTERNAL_ERROR])
def test_failover_aborted_requires_ambiguous_write(first_status):
    client, survivor = Mock(), Mock()
    updates = [PileState(pile_name="r1", state=PileState.DISCONNECTED), PileState(pile_name="r2", state=PileState.PRIMARY)]
    statuses = [StatusIds.ABORTED] if first_status is None else [first_status, StatusIds.ABORTED]
    client.update_cluster_state.side_effect = [bridge.UpdateClusterStateResponse(operation={"status": s}) for s in statuses]

    def state_at(generation, piles):
        reply = bridge.GetClusterStateResponse(operation={"status": StatusIds.SUCCESS})
        reply.operation.result.Pack(bridge.GetClusterStateResult(generation=generation, pile_states=piles))
        return reply

    if first_status == StatusIds.INTERNAL_ERROR:
        initial = [PileState(pile_name="r1", state=PileState.PRIMARY), PileState(pile_name="r2", state=PileState.SYNCHRONIZED)]
        survivor.get_cluster_state.side_effect = [state_at(1, initial), state_at(2, updates)]
        BridgeKiKiMRTest().update_and_confirm_failover(client, survivor, updates, 1)
        assert survivor.get_cluster_state.call_count == 2
    else:
        survivor.get_cluster_state.return_value = state_at(2, updates)
        with pytest.raises(AssertionError):
            BridgeKiKiMRTest().update_and_confirm_failover(client, survivor, updates, 1)
        survivor.get_cluster_state.assert_not_called()
    assert client.update_cluster_state.call_args_list == [call(updates, expected_generation=1)] * len(statuses)


class TestBridgeBasic(BridgeKiKiMRTest):

    def test_primary_pile_full_restart_recovers_system_tablets(self):
        primary_nodes = self.pile_nodes["r1"]
        with ThreadPoolExecutor(max_workers=len(primary_nodes)) as executor:
            list(executor.map(daemon.Daemon.kill, primary_nodes))
        assert all(not node.is_alive() for node in primary_nodes)

        with ThreadPoolExecutor(max_workers=len(primary_nodes)) as executor:
            list(executor.map(lambda node: node.start(), primary_nodes))

        node = primary_nodes[0]
        deadline = time.monotonic() + 120
        last_error = None
        while time.monotonic() < deadline:
            driver = ydb.Driver(ydb.DriverConfig(
                endpoint=f"grpc://{node.endpoint}", database="/Root", disable_discovery=True,
            ))
            pool = ydb.SessionPool(driver, size=1)
            try:
                pool.retry_operation_sync(
                    lambda session: session.execute_scheme(
                        "CREATE TABLE IF NOT EXISTS `/Root/after_primary_restart` (id Uint64, PRIMARY KEY(id));",
                        settings=ydb.BaseRequestSettings().with_timeout(10),
                    ),
                    retry_settings=ydb.RetrySettings(max_retries=0, get_session_client_timeout=5),
                )
                return
            except (grpc.RpcError, ydb.Error, ydb.SessionPoolEmpty) as error:
                last_error = error
                time.sleep(0.5)
            finally:
                pool.stop()
                self.stop_driver(driver)
        raise AssertionError("system tablets did not recover after the PRIMARY pile restart") from last_error

    def test_update_and_get_cluster_state(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})

        updates = [
            PileState(pile_name="r2", state=PileState.PROMOTED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state_any(
            self.bridge_client,
            [
                {"r1": PileState.PRIMARY, "r2": PileState.PROMOTED},
                {"r1": PileState.SYNCHRONIZED, "r2": PileState.PRIMARY},
            ],
        )

    def test_failover(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})

        updates = [
            PileState(pile_name="r1", state=PileState.DISCONNECTED),
            PileState(pile_name="r2", state=PileState.PRIMARY),
        ]
        self.update_and_confirm_failover(
            self.bridge_client, self.secondary_bridge_client, updates, expected_generation=initial_result.generation,
        )

    def test_takedown(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})
        updates = [
            PileState(pile_name="r2", state=PileState.SUSPENDED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.DISCONNECTED})

    def test_cli_takedown_of_primary(self):
        # the primary pile is taken down with the CLI naming the new primary; the request must not lose the primary
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})
        node = self.pile_nodes["r2"][0]
        # -y: admin commands ask for confirmation, and without a terminal the answer is no
        command = [self.configurator.get_ydb_cli_path(), '-y', '--endpoint', 'grpc://{}:{}'.format(node.host, node.port),
                   'admin', 'cluster', 'bridge', 'takedown', '--pile', 'r1', '--new-primary', 'r2']
        env = os.environ.copy()
        env['YDB_TOKEN'] = 'root@builtin'
        result = subprocess.run(command, capture_output=True, env=env, timeout=300)
        output = (result.stdout + result.stderr).decode('utf-8', errors='replace')
        assert result.returncode == 0, output
        self.wait_for_cluster_state(self.secondary_bridge_client, {"r1": PileState.DISCONNECTED, "r2": PileState.PRIMARY},
                                    timeout_seconds=60)

    def test_rejoin(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})
        updates = [
            PileState(pile_name="r2", state=PileState.SUSPENDED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.DISCONNECTED})
        updates = [
            PileState(pile_name="r2", state=PileState.NOT_SYNCHRONIZED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        # resynchronization of every group takes a while on a loaded machine
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED}, timeout_seconds=300)


class TestBridgeValidation(BridgeKiKiMRTest):

    @pytest.mark.parametrize(
        "updates, test_name",
        [
            (
                [],
                "no_updates"
            ),
            (
                [
                    PileState(pile_name="r1", state=PileState.PRIMARY),
                    PileState(pile_name="r2", state=PileState.PRIMARY),
                ],
                "multiple_primary_piles_in_request"
            ),
            (
                [
                    PileState(pile_name="r1", state=PileState.SYNCHRONIZED),
                ],
                "no_primary_pile_in_result"
            ),
            (
                [
                    PileState(pile_name="r1", state=PileState.SYNCHRONIZED),
                    PileState(pile_name="r1", state=PileState.PRIMARY),
                ],
                "duplicate_pile_update"
            ),
            (
                [
                    PileState(pile_name="r3", state=PileState.PRIMARY),
                ],
                "invalid_pile_name"
            ),
        ]
    )
    def test_invalid_updates(self, updates, test_name):
        self.logger.info(f"Running validation test: {test_name}")
        self.update_cluster_state(self.bridge_client, updates, StatusIds.BAD_REQUEST)
