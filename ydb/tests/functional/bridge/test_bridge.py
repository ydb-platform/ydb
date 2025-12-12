# -*- coding: utf-8 -*-
import pytest
import time
from hamcrest import assert_that

from common import BridgeKiKiMRTest

from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_bridge_client import bridge_client_factory
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start


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

    def _stop_pile_nodes(self, pile_nodes):
        """Останавливает ноды pile через node.stop()."""
        for node in pile_nodes:
            self.logger.info("Stopping storage node %d on host %s", node.node_id, node.host)
            node.stop()
        self.logger.info("Stopped %d storage nodes", len(pile_nodes))

    def _start_pile_nodes(self, pile_nodes):
        """Запускает ноды pile через node.start()."""
        for node in pile_nodes:
            self.logger.info("Starting storage node %d on host %s", node.node_id, node.host)
            node.start()
        self.logger.info("Started %d storage nodes", len(pile_nodes))

    def _value_for(self, key, tablet_id):
        """Вспомогательная функция для создания значения KV записи."""
        return "Value: <key = {key}, tablet_id = {tablet_id}>".format(
            key=key, tablet_id=tablet_id)

    def _check_cluster_kv_operations(self, table_path, tablet_ids):
        """
        Проверяет работоспособность кластера через KV операции записи/чтения.
        Аналогично check_kikimr_is_operational из test_distconf.py
        """
        self.logger.info("=== CHECKING CLUSTER KV OPERATIONS ===")
        for partition_id, tablet_id in enumerate(tablet_ids):
            # Запись
            write_resp = self.cluster.kv_client.kv_write(
                table_path, partition_id, "key", self._value_for("key", tablet_id)
            )
            assert_that(write_resp.operation.status == StatusIds.SUCCESS)
            self.logger.info("✓ KV write successful for partition %d, tablet %d", partition_id, tablet_id)

            # Чтение
            read_resp = self.cluster.kv_client.kv_read(
                table_path, partition_id, "key"
            )
            assert_that(read_resp.operation.status == StatusIds.SUCCESS)
            self.logger.info("✓ KV read successful for partition %d, tablet %d", partition_id, tablet_id)

        self.logger.info("✓ Cluster KV operations are working correctly")

    def test_failover_after_stopping_primary_pile_nodes(self):
        """
        Сценарий 1: ломаем Primary pile, делаем failover, проверяем состояние, восстанавливаем.

        Шаги сценария:
        1. Проверяем начальное состояние кластера (r1=PRIMARY, r2=SYNCHRONIZED)
        2. Создаем KV таблицу для проверки работоспособности (нагрузка на кластер)
        3. Проверяем работоспособность до failover через KV операции
        4. Останавливаем ноды primary pile (r1) - pile сломался
           - Если была нагрузка на кластер, могут быть ошибки связанные со сломанностью кластера
        5. Делаем failover на сломанный pile (r1)
           - failover() автоматически определяет, что r1 был PRIMARY
           - Переключает primary на другой pile (r2 становится PRIMARY)
           - r1 переходит в DISCONNECTED
        6. Проверяем состояние после failover через /bridge_list (get_cluster_state)
           - Должны получить: r1=DISCONNECTED, r2=PRIMARY
        7. Проверяем, что кластер все еще работает
           - Если была нагрузка, ошибки связанные с нагрузкой должны пропасть
           - Проверяем работоспособность через KV операции (запись/чтение)
        8. Восстанавливаем ноды primary pile (чиним что сломали)
        9. Вызываем rejoin на сломанный pile (r1)
           - r1 переходит из DISCONNECTED в NOT_SYNCHRONIZED, затем в SYNCHRONIZED
        10. Проверяем финальное состояние: r1=SYNCHRONIZED, r2=PRIMARY
        11. Проверяем работоспособность через KV операции после rejoin
            - Проверяем что после починки кластер работает, все наши проверки в т.ч. kv
        """
        # Шаг 1: Проверяем начальное состояние
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})
        self.logger.info("✓ Initial state: r1=PRIMARY, r2=SYNCHRONIZED")

        # Шаг 1.5: Создаем KV таблицу для проверки работоспособности кластера (нагрузка)
        self.logger.info("=== CREATING KV TABLE FOR OPERATIONAL CHECKS ===")
        table_path = '/Root/test_bridge_operational_primary'
        number_of_tablets = 3
        swagger_client = SwaggerClient(self.cluster.nodes[1].host, self.cluster.nodes[1].mon_port)
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )
        self.logger.info("✓ Created KV table %s with %d tablets", table_path, len(tablet_ids))

        # Проверяем работоспособность до failover
        self._check_cluster_kv_operations(table_path, tablet_ids)

        # Шаг 2: Получаем ноды primary pile (r1)
        primary_pile_nodes = self._get_nodes_for_pile_name("r1")
        if not primary_pile_nodes:
            pytest.fail("Could not find nodes for primary pile r1")
        self.logger.info(f"Found {len(primary_pile_nodes)} nodes for primary pile r1")

        # Шаг 3: Останавливаем ноды primary pile (pile сломался)
        self.logger.info("=== STOPPING PRIMARY PILE NODES ===")
        self._stop_pile_nodes(primary_pile_nodes)
        self.logger.info("✓ Primary pile nodes stopped (pile сломался)")

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

        # Шаг 5: Делаем failover на сломанный pile (используем метод failover())
        # failover() автоматически переключит primary на другой pile, если сломанный был primary
        self.logger.info("=== EXECUTING FAILOVER ===")
        self.logger.info("Current bridge state before failover:")
        state_before = self.get_cluster_state(secondary_bridge_client)
        self.logger.info(f"State: {[(s.pile_name, s.state) for s in state_before.pile_states]}")

        # Используем метод failover() - он сам определит, что r1 был PRIMARY и переключит r2 в PRIMARY
        result = secondary_bridge_client.failover("r1")
        if not result:
            pytest.fail("Failed to execute failover on r1")
        self.logger.info("✓ Failover command executed (переключил primary на другой pile)")

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
        self.logger.info("=== CHECKING CLUSTER AVAILABILITY ===")
        try:
            self.get_cluster_state(secondary_bridge_client)
            self.logger.info("✓ Cluster is still accessible and working")
        except Exception as e:
            pytest.fail(f"Cluster is not accessible after failover: {e}")

        # Шаг 7.5: Проверяем работоспособность через KV операции после failover
        # (ошибки связанные с нагрузкой должны пропасть, кластер работает)
        self._check_cluster_kv_operations(table_path, tablet_ids)

        # Шаг 8: Восстанавливаем ноды primary pile (чиним что сломали)
        self.logger.info("=== RESTORING PRIMARY PILE NODES ===")
        self._start_pile_nodes(primary_pile_nodes)
        self.logger.info("✓ Primary pile nodes started")

        # Даем время нодам запуститься
        time.sleep(5)

        # Шаг 9: Вызываем rejoin на сломанный pile (r1)
        self.logger.info("=== EXECUTING REJOIN ===")
        result = secondary_bridge_client.rejoin("r1")
        if not result:
            pytest.fail("Failed to execute rejoin on r1")
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

        # Финальная проверка работоспособности через KV операции после rejoin
        # (проверяем что после починки кластер работает, все наши проверки в т.ч. kv)
        self._check_cluster_kv_operations(table_path, tablet_ids)

        self.logger.info("✓ Test completed successfully")

    def test_failover_after_stopping_synchronized_pile_nodes(self):
        """
        Сценарий 2: ломаем Synchronized pile, делаем failover, проверяем состояние, восстанавливаем.

        Шаги сценария:
        1. Проверяем начальное состояние кластера (r1=PRIMARY, r2=SYNCHRONIZED)
        2. Создаем KV таблицу для проверки работоспособности (нагрузка на кластер)
        3. Проверяем работоспособность до failover через KV операции
        4. Останавливаем ноды synchronized pile (r2) - pile сломался
           - Если была нагрузка на кластер, могут быть ошибки связанные со сломанностью кластера
        5. Делаем failover на сломанный pile (r2)
           - failover() определяет, что r2 не был PRIMARY
           - r2 переходит в DISCONNECTED
           - r1 остается PRIMARY (не переключается, так как r2 не был PRIMARY)
        6. Проверяем состояние после failover через /bridge_list (get_cluster_state)
           - Должны получить: r1=PRIMARY, r2=DISCONNECTED
        7. Проверяем, что кластер все еще работает
           - Если была нагрузка, ошибки связанные с нагрузкой должны пропасть
           - Проверяем работоспособность через KV операции (запись/чтение)
        8. Восстанавливаем ноды synchronized pile (чиним что сломали)
        9. Вызываем rejoin на сломанный pile (r2)
           - r2 переходит из DISCONNECTED в NOT_SYNCHRONIZED, затем в SYNCHRONIZED
        10. Проверяем финальное состояние: r1=PRIMARY, r2=SYNCHRONIZED
        11. Проверяем работоспособность через KV операции после rejoin
            - Проверяем что после починки кластер работает, все наши проверки в т.ч. kv
        """
        # Шаг 1: Проверяем начальное состояние
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED})
        self.logger.info("✓ Initial state: r1=PRIMARY, r2=SYNCHRONIZED")

        # Шаг 1.5: Создаем KV таблицу для проверки работоспособности кластера (нагрузка)
        self.logger.info("=== CREATING KV TABLE FOR OPERATIONAL CHECKS ===")
        table_path = '/Root/test_bridge_operational_synchronized'
        number_of_tablets = 3
        swagger_client = SwaggerClient(self.cluster.nodes[1].host, self.cluster.nodes[1].mon_port)
        tablet_ids = create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )
        self.logger.info("✓ Created KV table %s with %d tablets", table_path, len(tablet_ids))

        # Проверяем работоспособность до failover
        self._check_cluster_kv_operations(table_path, tablet_ids)

        # Шаг 2: Получаем ноды synchronized pile (r2)
        synchronized_pile_nodes = self._get_nodes_for_pile_name("r2")
        if not synchronized_pile_nodes:
            pytest.fail("Could not find nodes for synchronized pile r2")
        self.logger.info(f"Found {len(synchronized_pile_nodes)} nodes for synchronized pile r2")

        # Шаг 3: Останавливаем ноды synchronized pile (pile сломался)
        self.logger.info("=== STOPPING SYNCHRONIZED PILE NODES ===")
        self._stop_pile_nodes(synchronized_pile_nodes)
        self.logger.info("✓ Synchronized pile nodes stopped (pile сломался)")

        # Даем время кластеру обнаружить, что ноды недоступны
        time.sleep(5)

        # Шаг 4: Получаем bridge_client для primary pile (r1) для выполнения failover
        primary_pile_nodes = self._get_nodes_for_pile_name("r1")
        if not primary_pile_nodes:
            pytest.fail("Could not find nodes for primary pile r1")

        primary_bridge_client = bridge_client_factory(
            primary_pile_nodes[0].host,
            primary_pile_nodes[0].port,
            cluster=self.cluster,
            retry_count=3,
            timeout=5
        )
        primary_bridge_client.set_auth_token('root@builtin')

        # Шаг 5: Делаем failover на сломанный pile (r2)
        # failover() переведет r2 в DISCONNECTED, но r1 останется PRIMARY (так как r2 не был PRIMARY)
        self.logger.info("=== EXECUTING FAILOVER ===")
        self.logger.info("Current bridge state before failover:")
        state_before = self.get_cluster_state(primary_bridge_client)
        self.logger.info(f"State: {[(s.pile_name, s.state) for s in state_before.pile_states]}")

        # Используем метод failover() - он переведет r2 в DISCONNECTED, но не изменит PRIMARY
        result = primary_bridge_client.failover("r2")
        if not result:
            pytest.fail("Failed to execute failover on r2")
        self.logger.info("✓ Failover command executed")

        # Шаг 6: Проверяем состояние после failover
        # /bridge_list должна вернуть что pile r2 в disconnected, r1 остается primary
        self.logger.info("=== CHECKING STATE AFTER FAILOVER ===")
        self.wait_for_cluster_state(
            primary_bridge_client,
            {"r1": PileState.PRIMARY, "r2": PileState.DISCONNECTED},
            timeout_seconds=30
        )
        self.logger.info("✓ State after failover: r1=PRIMARY, r2=DISCONNECTED")

        # Проверяем текущее состояние через get_cluster_state (аналог /bridge_list)
        state_after_failover = self.get_cluster_state(primary_bridge_client)
        actual_states = {s.pile_name: s.state for s in state_after_failover.pile_states}
        self.logger.info(f"Bridge list state: {actual_states}")

        assert actual_states.get("r1") == PileState.PRIMARY, \
            f"Expected r1 to remain PRIMARY, got {actual_states.get('r1')}"
        assert actual_states.get("r2") == PileState.DISCONNECTED, \
            f"Expected r2 to be DISCONNECTED, got {actual_states.get('r2')}"

        # Шаг 7: Проверяем, что кластер все еще работает
        self.logger.info("=== CHECKING CLUSTER AVAILABILITY ===")
        try:
            self.get_cluster_state(primary_bridge_client)
            self.logger.info("✓ Cluster is still accessible and working")
        except Exception as e:
            pytest.fail(f"Cluster is not accessible after failover: {e}")

        # Шаг 7.5: Проверяем работоспособность через KV операции после failover
        # (ошибки связанные с нагрузкой должны пропасть, кластер работает)
        self._check_cluster_kv_operations(table_path, tablet_ids)

        # Шаг 8: Восстанавливаем ноды synchronized pile (чиним что сломали)
        self.logger.info("=== RESTORING SYNCHRONIZED PILE NODES ===")
        self._start_pile_nodes(synchronized_pile_nodes)
        self.logger.info("✓ Synchronized pile nodes started")

        # Даем время нодам запуститься
        time.sleep(5)

        # Шаг 9: Вызываем rejoin на сломанный pile (r2)
        self.logger.info("=== EXECUTING REJOIN ===")
        result = primary_bridge_client.rejoin("r2")
        if not result:
            pytest.fail("Failed to execute rejoin on r2")
        self.logger.info("✓ Rejoin command executed")

        # Ждем, пока r2 вернется в SYNCHRONIZED
        self.logger.info("=== WAITING FOR REJOIN TO COMPLETE ===")
        self.wait_for_cluster_state(
            primary_bridge_client,
            {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED},
            timeout_seconds=50
        )
        self.logger.info("✓ Rejoin completed: r1=PRIMARY, r2=SYNCHRONIZED")

        # Финальная проверка состояния
        final_state = self.get_cluster_state(primary_bridge_client)
        final_states = {s.pile_name: s.state for s in final_state.pile_states}
        self.logger.info(f"Final bridge list state: {final_states}")

        assert final_states.get("r1") == PileState.PRIMARY, \
            f"Expected r1 to remain PRIMARY, got {final_states.get('r1')}"
        assert final_states.get("r2") == PileState.SYNCHRONIZED, \
            f"Expected r2 to be SYNCHRONIZED after rejoin, got {final_states.get('r2')}"

        # Финальная проверка работоспособности через KV операции после rejoin
        # (проверяем что после починки кластер работает, все наши проверки в т.ч. kv)
        self._check_cluster_kv_operations(table_path, tablet_ids)

        self.logger.info("✓ Test completed successfully")
