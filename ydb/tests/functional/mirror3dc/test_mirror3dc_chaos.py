from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
import logging
import random
import threading
import time

import grpc
import yatest.common

from ydb.tests.library.clients.kikimr_client import KiKiMRMessageBusClient
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.functional.mirror3dc.kv_churn import KvChurnWorkload
from ydb.tests.functional.mirror3dc.mirror3dc_base import Mirror3dcTestBase


logger = logging.getLogger(__name__)


class TestMirror3dcChaos(Mirror3dcTestBase):
    # SectorMap is process-local; restarting a node must preserve its PDisks.
    USE_IN_MEMORY_PDISKS = False
    PARTITIONS = 1
    ENABLE_NODE_RESTARTS = False
    ADDITIONAL_LOG_CONFIGS = {
        component: LogLevels.DEBUG for component in (
            'BS_CONTROLLER',
            'BS_PROXY', 'BS_PROXY_PUT', 'BS_PROXY_GET', 'BS_PROXY_MULTIGET',
            'BS_PROXY_INDEXRESTOREGET', 'BS_PROXY_DISCOVER', 'BS_PROXY_RANGE',
            'BS_PROXY_BLOCK', 'BS_PROXY_GETBLOCK', 'BS_PROXY_COLLECT',
            'BS_PROXY_MULTICOLLECT', 'BS_PROXY_STATUS', 'BS_PROXY_PATCH',
            'BS_PROXY_ASSIMILATE', 'BS_PROXY_CHECKINTEGRITY',
        )
    }
    DC_CHANGE_INTERVAL = 0.1
    DC_CHANGE_POLL_INTERVAL = 0.005
    STATE_POLL_INTERVAL = 0.1
    ACTION_INTERVAL = 0.01
    NODE_RESTART_INTERVAL = (0.1, 0.3)
    NODE_DOWNTIME = (0.2, 0.4)

    def setUp(self):
        self.control_clients = {}
        super().setUp()
        self.actions_log_path = yatest.common.output_path('mirror3dc_chaos_actions.log')
        handler = logging.FileHandler(self.actions_log_path, mode='w', encoding='utf-8')
        handler.setFormatter(logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(threadName)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        ))
        self.actions_logger = logging.Logger(f'{__name__}.actions', level=logging.INFO)
        self.actions_logger.addHandler(handler)
        self.addCleanup(handler.close)
        self.actions_logger.info('CONTROL CLUSTER_TOPOLOGY nodes_by_dc=%s', {
            dc: sorted(node_id for node_id, node_dc in self.node_data_centers.items() if node_dc == dc)
            for dc in self.data_centers
        })
        logger.info('Chaos actions and KV log: %s', self.actions_log_path)
        self.topology_lock = threading.Lock()
        self.down_node = None
        self.last_change = None
        self.node_restarts = 0
        self.changes = Counter()
        # The harness client always uses node 1 and is reset by node.stop().
        # Own clients let BSC requests use any currently running node.
        for node_id, node in self.cluster.nodes.items():
            client = KiKiMRMessageBusClient(node.host, node.grpc_port, retry_count=1, timeout=5)
            self.control_clients[node_id] = client
            self.addCleanup(client.close)

    @property
    def bsc_client(self):
        if not self.control_clients:
            return super().bsc_client
        node_id = next(node_id for node_id in self.recovery_node_ids if node_id != self.down_node)
        return self.control_clients[node_id]

    def _states(self):
        with self.topology_lock:
            return self._states_locked()

    def _states_locked(self):
        group = self.get_group()
        states = {realm.DataCenter: realm.State for realm in group.FailRealms}
        self.assertEqual(len(group.FailRealms), 3, str(group))
        self.assertEqual(set(states), set(self.data_centers), str(group))
        self.actions_logger.info(
            'CONTROL GROUP_STATE group=%s generation=%s state=%s states=%s',
            self.group_id,
            group.GroupGeneration,
            self.GROUP_STATES.EState.Name(group.Mirror3dcState),
            {dc: self.DC_STATES.EState.Name(state) for dc, state in sorted(states.items())},
        )
        logger.info(
            'Chaos group %s generation %s state %s: %s',
            self.group_id,
            group.GroupGeneration,
            self.GROUP_STATES.EState.Name(group.Mirror3dcState),
            {dc: self.DC_STATES.EState.Name(state) for dc, state in states.items()},
        )
        return group.Mirror3dcState, states

    def _check_background(self, workload, node_task):
        workload.check_errors()
        if node_task.done():
            node_task.result()

    def _log_topology(self):
        # Called under topology_lock after the action has completed.
        self.actions_logger.info('CONTROL CLUSTER_STATE included_dcs=%s excluded_dcs=%s stopped_nodes=%s',
                                 sorted(set(self.data_centers) - self.excluded_dcs),
                                 sorted(self.excluded_dcs),
                                 [] if self.down_node is None else [self.down_node])

    def _change_dc(self, dc, state, workload, node_task):
        # Count from completion of the previous command, including cleanup.
        while self.last_change is not None and time.monotonic() - self.last_change < self.DC_CHANGE_INTERVAL:
            self._check_background(workload, node_task)
            time.sleep(self.DC_CHANGE_POLL_INTERVAL)
        self._check_background(workload, node_task)
        with self.topology_lock:
            # Recovery waits below hold this lock, so let a stopped node return first.
            if self.down_node is not None:
                return False
            previous = len(self.excluded_dcs)
            now = time.monotonic()
            if self.last_change is not None:
                self.assertGreaterEqual(now - self.last_change, self.DC_CHANGE_INTERVAL)
            self.actions_logger.info('CONTROL DC_CHANGE_REQUEST dc=%s state=%s', dc, self.DC_STATES.EState.Name(state))
            if state == self.DC_STATES.INCLUDED:
                self.return_dc(dc)
            else:
                self.define_dc_state(dc, state)
                if len(self.excluded_dcs) == 1:
                    self.wait_for_two_copy_recovery()
            self.actions_logger.info('CONTROL DC_CHANGE_OK dc=%s excluded=%s', dc, sorted(self.excluded_dcs))
            self._log_topology()
            self.last_change = time.monotonic()
            action = 'include' if state == self.DC_STATES.INCLUDED else f'exclude_{previous + 1}'
            self.changes[action] += 1
            logger.info('Chaos DC action %s: dc=%s, excluded=%s, workload=%s',
                        action, dc, sorted(self.excluded_dcs), workload.stats())
        return True

    def _wait_node_ready(self, node_id, previous_connect_timestamp):
        deadline = time.monotonic() + 240
        while True:
            try:
                with self.topology_lock:
                    base = self.bsc_client.query_base_config().BaseConfig
                node, = (node for node in base.Node if node.NodeId == node_id)
                slots = [slot for slot in base.VSlot if slot.VSlotId.NodeId == node_id]
                if (node.Connected and node.LastConnectTimestamp > previous_connect_timestamp
                        and slots and all(slot.Status == 'READY' and slot.Ready for slot in slots)):
                    return
            except grpc.RpcError as error:
                if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    raise
            self.assertLess(time.monotonic(), deadline, f'Node {node_id} did not reconnect with ready VDisks')
            time.sleep(self.STATE_POLL_INTERVAL)

    def _restart_nodes(self, stopped, seed):
        rng = random.Random(seed)
        while not stopped.wait(rng.uniform(*self.NODE_RESTART_INTERVAL)):
            node_id = None
            try:
                with self.topology_lock:
                    if len(self.excluded_dcs) == 2:
                        group_state, states = self._states_locked()
                        live_dc, = set(self.data_centers) - self.excluded_dcs
                        if (
                            group_state != self.GROUP_STATES.DC_SOLO_INV2
                            or states[live_dc] != self.DC_STATES.INCLUDED
                        ):
                            self.actions_logger.info(
                                'CONTROL NODE_STOP_SKIPPED dc=%s state=%s reason=solo_group_not_INV2',
                                live_dc,
                                self.GROUP_STATES.EState.Name(group_state),
                            )
                            continue
                    node_id = rng.choice(self.recovery_node_ids)
                    base = self.bsc_client.query_base_config().BaseConfig
                    previous_connect_timestamp = next(node.LastConnectTimestamp for node in base.Node if node.NodeId == node_id)
                    self.down_node = node_id
                    logger.info('Chaos stopping node %s in live DC %s', node_id, self.node_data_centers[node_id])
                    self.actions_logger.info('CONTROL NODE_STOP_REQUEST node=%s dc=%s', node_id, self.node_data_centers[node_id])
                    self.cluster.nodes[node_id].stop()
                    self.actions_logger.info('CONTROL NODE_STOPPED node=%s dc=%s', node_id, self.node_data_centers[node_id])
                    self._log_topology()
                stopped.wait(rng.uniform(*self.NODE_DOWNTIME))
            finally:
                if node_id is not None:
                    with self.topology_lock:
                        self.actions_logger.info('CONTROL NODE_START_REQUEST node=%s dc=%s', node_id, self.node_data_centers[node_id])
                        self.cluster.nodes[node_id].start()
                        self.actions_logger.info('CONTROL NODE_STARTED node=%s dc=%s', node_id, self.node_data_centers[node_id])
                    self._wait_node_ready(node_id, previous_connect_timestamp)
                    with self.topology_lock:
                        self.down_node = None
                        self.actions_logger.info('CONTROL NODE_READY node=%s dc=%s', node_id, self.node_data_centers[node_id])
                        self._log_topology()
                        self.node_restarts += 1
                        logger.info('Chaos node %s returned, restart count=%d', node_id, self.node_restarts)

    def _wait_states(self, expected_group_state, expected_roles, workload, node_task):
        deadline = time.monotonic() + 240
        while True:
            self._check_background(workload, node_task)
            actual = self._states()
            expected = expected_group_state, expected_roles
            if actual == expected:
                return
            self.assertLess(
                time.monotonic(), deadline, f'Group configuration did not converge: {actual}, expected {expected}'
            )
            time.sleep(self.STATE_POLL_INTERVAL)

    def _random_transitions(self, workload, node_task, rng, duration):
        deadline = time.monotonic() + duration
        pending = None
        last_report = time.monotonic()
        last_query_success = time.monotonic()
        while time.monotonic() < deadline:
            self._check_background(workload, node_task)
            try:
                group_state, states = self._states()
                last_query_success = time.monotonic()
            except grpc.RpcError as error:
                # A BSC tablet can migrate when its host is restarted.
                if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    raise
                if time.monotonic() - last_query_success > 30:
                    raise
                time.sleep(self.STATE_POLL_INTERVAL)
                continue
            if time.monotonic() - last_report >= 5:
                logger.info('Chaos progress: actions=%s, node_restarts=%d, workload=%s',
                            dict(self.changes), self.node_restarts, workload.stats())
                last_report = time.monotonic()
            # Never start a second recovery while a realm is already R.
            if self.DC_STATES.RECOVERING in states.values():
                time.sleep(self.STATE_POLL_INTERVAL)
                continue
            excluded = sorted(self.excluded_dcs)
            if pending is None:
                if not excluded:
                    if group_state != self.GROUP_STATES.NORMAL or not all(
                        state == self.DC_STATES.INCLUDED for state in states.values()
                    ):
                        time.sleep(self.STATE_POLL_INTERVAL)
                        continue
                    pending = (rng.choice(self.data_centers), self.DC_STATES.EXCLUDED)
                elif len(excluded) == 1:
                    if rng.choice([True, False]):
                        pending = (excluded[0], self.DC_STATES.INCLUDED)
                    else:
                        pending = (rng.choice([dc for dc in self.data_centers if dc not in excluded]),
                                   self.DC_STATES.EXCLUDED)
                else:
                    pending = (rng.choice(excluded), self.DC_STATES.INCLUDED)
            dc, state = pending
            if state == self.DC_STATES.EXCLUDED and excluded:
                # Exclude the second DC only after BSC confirms the group INV2 guarantee.
                if group_state != self.GROUP_STATES.DC_FAILURE_INV2 or states != self.recovered_realm_states():
                    time.sleep(self.STATE_POLL_INTERVAL)
                    continue
            if self._change_dc(dc, state, workload, node_task):
                pending = None
            time.sleep(self.ACTION_INTERVAL)

    def _test_random_dc_transitions_and_node_restarts(self):
        seed = int(yatest.common.get_param('mirror3dc_chaos_seed', '20261030'))
        duration = float(yatest.common.get_param('mirror3dc_chaos_seconds', '180'))
        self.assertGreater(duration, 0)
        logger.info('Mirror3dc chaos seed=%d, duration=%s seconds', seed, duration)
        self.actions_logger.info('CONTROL TEST_START seed=%d duration=%s', seed, duration)
        stop_faults = threading.Event()
        with KvChurnWorkload(
            [node.endpoint for node in self.cluster.nodes.values()],
            self.path,
            seed=seed,
            readers=1,
            event_logger=self.actions_logger,
        ) as workload:
            deadline = time.monotonic() + 30
            while not workload.stats()['deletes']:
                workload.check_errors()
                self.assertLess(time.monotonic(), deadline, 'No KV deletions before starting faults')
                time.sleep(self.ACTION_INTERVAL)
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix='mirror3dc-node-fault') as executor:
                if self.ENABLE_NODE_RESTARTS:
                    node_task = executor.submit(self._restart_nodes, stop_faults, seed + 1)
                else:
                    self.actions_logger.info('CONTROL NODE_RESTARTS_DISABLED')
                    node_task = Future()
                    node_task.set_result(None)
                try:
                    self._random_transitions(workload, node_task, random.Random(seed), duration)
                finally:
                    stop_faults.set()
                    node_task.result()  # Always return a physically stopped node.
            # Keep KV traffic running during sequential DC cleanup as well.
            while self.excluded_dcs:
                deadline = time.monotonic() + 240
                while self.DC_STATES.RECOVERING in self._states()[1].values():
                    self._check_background(workload, node_task)
                    self.assertLess(time.monotonic(), deadline, 'DC recovery did not finish before cleanup')
                    time.sleep(self.STATE_POLL_INTERVAL)
                dc = sorted(self.excluded_dcs)[-1]
                self._change_dc(dc, self.DC_STATES.INCLUDED, workload, node_task)
                expected = (
                    self.recovered_realm_states()
                    if self.excluded_dcs
                    else {name: self.DC_STATES.INCLUDED for name in self.data_centers}
                )
                self._wait_states(self.recovered_group_state(), expected, workload, node_task)
            self._wait_states(
                self.GROUP_STATES.NORMAL,
                {dc: self.DC_STATES.INCLUDED for dc in self.data_centers},
                workload,
                node_task,
            )
            if self.ENABLE_NODE_RESTARTS:
                self.assertGreater(self.node_restarts, 0, 'No node restarts executed')
            self.assertGreater(self.changes['exclude_1'], 0, 'No DC exclusions executed')
        logger.info('Chaos finished: seed=%d, actions=%s, node_restarts=%d, workload=%s',
                    seed, dict(self.changes), self.node_restarts, workload.stats())
        self.actions_logger.info('CONTROL TEST_PASSED')
