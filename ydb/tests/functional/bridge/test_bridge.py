# -*- coding: utf-8 -*-
import pytest
import time

from common import BridgeKiKiMRTest

from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_bridge_client import bridge_client_factory


class TestBridgeBasic(BridgeKiKiMRTest):

    def test_update_and_get_cluster_state(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})

        updates = [
            PileState(pile_name="r2", state=PileState.PROMOTED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.PROMOTED})

    def test_failover(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})

        updates = [
            PileState(pile_name="r1", state=PileState.DISCONNECTED),
            PileState(pile_name="r2", state=PileState.PRIMARY),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.secondary_bridge_client, {"r1": PileState.DISCONNECTED, "r2": PileState.PRIMARY})

    def test_takedown(self):
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})
        updates = [
            PileState(pile_name="r2", state=PileState.SUSPENDED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.DISCONNECTED})

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
        self.wait_for_cluster_state(self.bridge_client, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED}, timeout_seconds=50)


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


class TestBridgeFailoverWithNodeStop(BridgeKiKiMRTest):
    """
    Тест реализует сценарий failover при остановке нод primary pile:
    1. Ломаем Primary pile (останавливаем ноды)
    2. Делаем failover на сломанный pile (переключает primary на другой pile)
    3. Проверяем после failover - /bridge_list должна вернуть что pile в disconnected
    4. Проверяем что кластер все еще работает
    5. Чиним что сломали и вызываем rejoin на сломанный pile
    """

    def _get_nodes_for_pile_name(self, pile_name):
        """Получает ноды, которые относятся к указанному pile по имени."""
        nodes_for_pile = []
        bridge_config = self.configurator.bridge_config
        if not bridge_config:
            return nodes_for_pile

        piles = bridge_config.get("piles", [])
        if not piles:
            return nodes_for_pile

        # Находим индекс pile по имени
        pile_index = None
        for idx, pile in enumerate(piles):
            if pile.get("name") == pile_name:
                pile_index = idx
                break

        if pile_index is None:
            return nodes_for_pile

        # Распределение нод по piles: (node_id - 1) % len(piles)
        # Логика из kikimr_config.py строка 927
        for node_id, node in self.cluster.nodes.items():
            assigned_pile_index = (node_id - 1) % len(piles)
            if assigned_pile_index == pile_index:
                nodes_for_pile.append(node)

        return nodes_for_pile

    def _get_pile_id_from_name(self, pile_name):
        """Получает pile_id из pile_name на основе bridge_config."""
        bridge_config = self.configurator.bridge_config
        if bridge_config:
            piles = bridge_config.get("piles", [])
            for idx, pile in enumerate(piles):
                if pile.get("name") == pile_name:
                    # pile_id может быть 0-based или 1-based, используем индекс + 1
                    return idx + 1
        return None

    def _stop_primary_pile_nodes(self, primary_pile_nodes):
        """Останавливает ноды primary pile через node.stop()."""
        for node in primary_pile_nodes:
            self.logger.info("Stopping storage node %d on host %s", node.node_id, node.host)
            node.stop()
        self.logger.info("Stopped %d storage nodes", len(primary_pile_nodes))

    def _start_primary_pile_nodes(self, primary_pile_nodes):
        """Запускает ноды primary pile через node.start()."""
        for node in primary_pile_nodes:
            self.logger.info("Starting storage node %d on host %s", node.node_id, node.host)
            node.start()
        self.logger.info("Started %d storage nodes", len(primary_pile_nodes))

    def test_failover_after_stopping_primary_pile_nodes(self):
        """
        Сценарий: ломаем Primary pile, делаем failover, проверяем состояние, восстанавливаем.
        """
        # Шаг 1: Проверяем начальное состояние
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})
        self.logger.info("✓ Initial state: r1=PRIMARY, r2=SYNCHRONIZED")

        # Шаг 2: Получаем ноды primary pile (r1)
        primary_pile_nodes = self._get_nodes_for_pile_name("r1")
        if not primary_pile_nodes:
            pytest.fail("Could not find nodes for primary pile r1")
        self.logger.info(f"Found {len(primary_pile_nodes)} nodes for primary pile r1")

        # Шаг 3: Останавливаем ноды primary pile
        self.logger.info("=== STOPPING PRIMARY PILE NODES ===")
        self._stop_primary_pile_nodes(primary_pile_nodes)
        self.logger.info("✓ Primary pile nodes stopped")

        # Даем время кластеру обнаружить, что ноды недоступны
        time.sleep(5)

        # Шаг 4: Получаем bridge_client для secondary pile (r2) для выполнения failover
        secondary_pile_nodes = self._get_nodes_for_pile_name("r2")
        if not secondary_pile_nodes:
            pytest.fail("Could not find nodes for secondary pile r2")

        secondary_bridge_client = bridge_client_factory(
            secondary_pile_nodes[0].host,
            secondary_pile_nodes[0].port,
            cluster=self.cluster,
            retry_count=3,
            timeout=5
        )
        secondary_bridge_client.set_auth_token('root@builtin')

        # Шаг 5: Получаем pile_id для r1 и делаем failover
        # Используем update_cluster_state (как в существующих тестах) вместо failover(),
        # так как failover() требует pile_id, а у нас pile_name
        self.logger.info("=== EXECUTING FAILOVER ===")
        self.logger.info("Current bridge state before failover:")
        state_before = self.get_cluster_state(secondary_bridge_client)
        self.logger.info(f"State: {[(s.pile_name, s.state) for s in state_before.pile_states]}")

        # Выполняем failover через update_cluster_state
        updates = [
            PileState(pile_name="r1", state=PileState.DISCONNECTED),
            PileState(pile_name="r2", state=PileState.PRIMARY),
        ]
        self.update_cluster_state(secondary_bridge_client, updates)
        self.logger.info("✓ Failover command executed")

        # Шаг 6: Проверяем состояние после failover
        # /bridge_list должна вернуть что pile r1 в disconnected, r2 в primary
        self.logger.info("=== CHECKING STATE AFTER FAILOVER ===")
        self.wait_for_cluster_state(
            secondary_bridge_client,
            {"r1": PileState.DISCONNECTED, "r2": PileState.PRIMARY},
            timeout_seconds=30
        )
        self.logger.info("✓ State after failover: r1=DISCONNECTED, r2=PRIMARY")

        # Проверяем текущее состояние через get_cluster_state (аналог /bridge_list)
        state_after_failover = self.get_cluster_state(secondary_bridge_client)
        actual_states = {s.pile_name: s.state for s in state_after_failover.pile_states}
        self.logger.info(f"Bridge list state: {actual_states}")

        assert actual_states.get("r1") == PileState.DISCONNECTED, \
            f"Expected r1 to be DISCONNECTED, got {actual_states.get('r1')}"
        assert actual_states.get("r2") == PileState.PRIMARY, \
            f"Expected r2 to be PRIMARY, got {actual_states.get('r2')}"

        # Шаг 7: Проверяем, что кластер все еще работает
        # Простая проверка - можем ли мы получить состояние кластера
        self.logger.info("=== CHECKING CLUSTER AVAILABILITY ===")
        try:
            self.get_cluster_state(secondary_bridge_client)
            self.logger.info("✓ Cluster is still accessible and working")
        except Exception as e:
            pytest.fail(f"Cluster is not accessible after failover: {e}")

        # Шаг 8: Восстанавливаем ноды primary pile
        self.logger.info("=== RESTORING PRIMARY PILE NODES ===")
        self._start_primary_pile_nodes(primary_pile_nodes)
        self.logger.info("✓ Primary pile nodes started")

        # Даем время нодам запуститься
        time.sleep(5)

        # Шаг 9: Вызываем rejoin на сломанный pile (r1)
        self.logger.info("=== EXECUTING REJOIN ===")
        # Используем update_cluster_state для rejoin (переводим r1 из DISCONNECTED в NOT_SYNCHRONIZED)
        rejoin_updates = [
            PileState(pile_name="r1", state=PileState.NOT_SYNCHRONIZED),
        ]
        self.update_cluster_state(secondary_bridge_client, rejoin_updates)
        self.logger.info("✓ Rejoin command executed")

        # Ждем, пока r1 вернется в SYNCHRONIZED
        self.logger.info("=== WAITING FOR REJOIN TO COMPLETE ===")
        self.wait_for_cluster_state(
            secondary_bridge_client,
            {"r1": PileState.SYNCHRONIZED, "r2": PileState.PRIMARY},
            timeout_seconds=50
        )
        self.logger.info("✓ Rejoin completed: r1=SYNCHRONIZED, r2=PRIMARY")

        # Финальная проверка состояния
        final_state = self.get_cluster_state(secondary_bridge_client)
        final_states = {s.pile_name: s.state for s in final_state.pile_states}
        self.logger.info(f"Final bridge list state: {final_states}")

        assert final_states.get("r1") == PileState.SYNCHRONIZED, \
            f"Expected r1 to be SYNCHRONIZED after rejoin, got {final_states.get('r1')}"
        assert final_states.get("r2") == PileState.PRIMARY, \
            f"Expected r2 to remain PRIMARY, got {final_states.get('r2')}"

        self.logger.info("✓ Test completed successfully")
