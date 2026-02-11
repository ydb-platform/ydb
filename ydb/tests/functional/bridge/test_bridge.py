# -*- coding: utf-8 -*-
import pytest
import time
import threading

from common import BridgeKiKiMRTest

from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_bridge_client import bridge_client_factory
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.clients.kikimr_keyvalue_client import keyvalue_client_factory
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start, get_kv_tablet_ids


class TestBridgeBasic(BridgeKiKiMRTest):

    def test_update_and_get_cluster_state(self):
        # Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state", timeout_seconds=60)

        # Ждем, пока кластер достигнет начального состояния
        self.wait_for_cluster_state(
            self.bridge_client,
            {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED},
            timeout_seconds=60
        )
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED})

        updates = [
            PileState(pile_name=secondary_pile_name, state=PileState.PROMOTED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.PROMOTED})

    def test_failover(self):
        # Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state", timeout_seconds=60)

        # Ждем, пока кластер достигнет начального состояния
        self.wait_for_cluster_state(
            self.bridge_client,
            {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED},
            timeout_seconds=60
        )
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED})

        updates = [
            PileState(pile_name=primary_pile_name, state=PileState.DISCONNECTED),
            PileState(pile_name=secondary_pile_name, state=PileState.PRIMARY),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.secondary_bridge_client, {primary_pile_name: PileState.DISCONNECTED, secondary_pile_name: PileState.PRIMARY})

    def test_takedown(self):
        # Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state", timeout_seconds=60)

        # Ждем, пока кластер достигнет начального состояния
        self.wait_for_cluster_state(
            self.bridge_client,
            {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED},
            timeout_seconds=60
        )
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED})
        updates = [
            PileState(pile_name=secondary_pile_name, state=PileState.SUSPENDED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.DISCONNECTED})

    def test_rejoin(self):
        # Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state", timeout_seconds=60)

        # Ждем, пока кластер достигнет начального состояния
        self.wait_for_cluster_state(
            self.bridge_client,
            {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED},
            timeout_seconds=60
        )
        initial_result = self.get_cluster_state(self.bridge_client)
        self.check_states(initial_result, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED})
        updates = [
            PileState(pile_name=secondary_pile_name, state=PileState.SUSPENDED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.DISCONNECTED})
        updates = [
            PileState(pile_name=secondary_pile_name, state=PileState.NOT_SYNCHRONIZED),
        ]
        self.update_cluster_state(self.bridge_client, updates)
        self.wait_for_cluster_state(self.bridge_client, {primary_pile_name: PileState.PRIMARY, secondary_pile_name: PileState.SYNCHRONIZED}, timeout_seconds=60)


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
    # Константы таймаутов для тестов
    TIMEOUT_CLUSTER_STATE_SECONDS = 60  # Таймаут для wait_for_cluster_state_with_step
    TIMEOUT_REJOIN_SECONDS = 60  # Таймаут для ожидания завершения rejoin

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

    def _determine_current_cluster_state(self, step_name="determining current cluster state"):
        """
        Определяет текущее состояние кластера и возвращает имена primary и secondary piles.
        Независимо от конкретных имен piles (r1/r2), определяет какой pile сейчас PRIMARY.

        Args:
            step_name: Название шага для логирования

        Returns:
            tuple: (primary_pile_name, secondary_pile_name)
        """
        self.logger.info(f"=== {step_name.upper()} ===")
        try:
            current_state = self.get_cluster_state(self.bridge_client)
            current_states = {s.pile_name: s.state for s in current_state.pile_states}
        except Exception as e:
            # Если не удалось получить состояние, ждем стабильного состояния
            self.logger.warning(f"Failed to get cluster state: {e}. Waiting for stable state...")
            # Проверяем, что есть один PRIMARY и один SYNCHRONIZED
            self.wait_for_cluster_state_with_step(
                self.bridge_client,
                {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED},
                step_name="waiting for stable state",
                timeout_seconds=self.TIMEOUT_CLUSTER_STATE_SECONDS
            )
            current_state = self.get_cluster_state(self.bridge_client)
            current_states = {s.pile_name: s.state for s in current_state.pile_states}

        # Определяем какой pile сейчас PRIMARY
        primary_pile_name = "r1" if current_states.get("r1") == PileState.PRIMARY else "r2"
        secondary_pile_name = "r2" if primary_pile_name == "r1" else "r1"

        self.logger.info(f"Current primary: {primary_pile_name}, secondary: {secondary_pile_name}")

        # Проверяем, что состояние корректное (один PRIMARY, один SYNCHRONIZED)
        primary_count = sum(1 for state in current_states.values() if state == PileState.PRIMARY)
        synchronized_count = sum(1 for state in current_states.values() if state == PileState.SYNCHRONIZED)
        assert primary_count == 1, \
            f"[Step: {step_name}] Expected exactly one PRIMARY, got {primary_count}. " \
            f"Full state: {current_states}"
        assert synchronized_count == 1, \
            f"[Step: {step_name}] Expected exactly one SYNCHRONIZED, got {synchronized_count}. " \
            f"Full state: {current_states}"

        self.logger.info(f"✓ Initial state: {primary_pile_name}=PRIMARY, {secondary_pile_name}=SYNCHRONIZED")
        return primary_pile_name, secondary_pile_name

    def _check_data_integrity(self, table_path, initial_data, step_name, kv_client=None, max_total_timeout_seconds=60):
        """
        Проверяет целостность данных через чтение всех записей из initial_data.
        С retry логикой для случаев, когда кластер временно недоступен после failover/rejoin.

        Args:
            table_path: Путь к KV таблице
            initial_data: Словарь {partition_id: (key, expected_value)} с данными для проверки
            step_name: Название шага теста для логирования (например, "checking data integrity after failover")
            kv_client: KeyValueClient для использования (если None, используется self.cluster.kv_client)
            max_total_timeout_seconds: Максимальное общее время на все попытки (по умолчанию 60 секунд)
        """
        context = f" (step: {step_name})" if step_name else ""
        self.logger.info("=== CHECKING DATA INTEGRITY%s ===", context)

        # Используем переданный клиент или дефолтный
        client_to_use = kv_client if kv_client is not None else self.cluster.kv_client

        start_time = time.time()
        retry_delay = 3  # Задержка между попытками
        attempt = 0

        while time.time() - start_time < max_total_timeout_seconds:
            attempt += 1
            try:
                all_success = True
                for partition_id, (key, expected_value) in initial_data.items():
                    try:
                        read_resp = client_to_use.kv_read(table_path, partition_id, key)
                        assert read_resp.operation.status == StatusIds.SUCCESS, \
                            f"[Step: {step_name}] KV read failed: partition={partition_id}, " \
                            f"key={key}, status={read_resp.operation.status}, table_path={table_path}"

                        if not hasattr(read_resp, 'operation'):
                            raise AssertionError(
                                f"[Step: {step_name}] KV read response does not contain 'operation' field: "
                                f"partition={partition_id}, key={key}, table_path={table_path}"
                            )

                        operation = read_resp.operation
                        if not hasattr(operation, 'result'):
                            raise AssertionError(
                                f"[Step: {step_name}] KV read response operation does not contain 'result' field: "
                                f"partition={partition_id}, key={key}, table_path={table_path}"
                            )

                        result = operation.result
                        if not hasattr(result, 'value'):
                            raise AssertionError(
                                f"[Step: {step_name}] KV read response result does not contain 'value' field: "
                                f"partition={partition_id}, key={key}, table_path={table_path}"
                            )

                        read_value = result.value

                        if isinstance(read_value, bytes):
                            read_value = read_value.decode('utf-8', errors='replace')
                        if isinstance(expected_value, bytes):
                            expected_value = expected_value.decode('utf-8')

                        # Извлекаем ожидаемое значение из строки (может содержать служебные символы)
                        if expected_value in read_value:
                            value_pos = read_value.find(expected_value)
                            read_value = read_value[value_pos:value_pos + len(expected_value)]

                        assert read_value == expected_value, \
                            f"[Step: {step_name}] Data integrity check failed: partition={partition_id}, " \
                            f"key={key}, expected={expected_value!r}, got={read_value!r}, table_path={table_path}"

                        self.logger.info(f"✓ Data integrity check passed for partition {partition_id}%s (value verified)", context)
                    except Exception as e:
                        all_success = False
                        elapsed_time = time.time() - start_time
                        if elapsed_time + retry_delay < max_total_timeout_seconds:
                            # Пробрасываем исключение для retry
                            break
                        else:
                            # Не хватает времени на еще одну попытку
                            raise AssertionError(
                                f"[Step: {step_name}] Failed to read initial data (attempt {attempt}, "
                                f"elapsed: {elapsed_time:.1f}s, timeout: {max_total_timeout_seconds}s): "
                                f"partition={partition_id}, key={key}, table_path={table_path}, error={e}"
                            ) from e

                if all_success:
                    elapsed_time = time.time() - start_time
                    self.logger.info("✓ All data integrity checks passed%s (took %.1fs)", context, elapsed_time)
                    return  # Все успешно прочитаны

            except AssertionError:
                elapsed_time = time.time() - start_time
                if elapsed_time + retry_delay < max_total_timeout_seconds:
                    self.logger.warning(
                        f"[Step: {step_name}] Data integrity check failed (attempt {attempt}, "
                        f"elapsed: {elapsed_time:.1f}s, timeout: {max_total_timeout_seconds}s). "
                        f"Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                else:
                    raise

        # Если вышли из цикла по таймауту
        elapsed_time = time.time() - start_time
        raise AssertionError(
            f"[Step: {step_name}] Failed to read initial data: timeout exceeded "
            f"(total time: {elapsed_time:.1f}s, max timeout: {max_total_timeout_seconds}s)"
        )

    def _check_cluster_kv_operations(self, table_path, tablet_ids, step_name="", kv_client=None, max_total_timeout_seconds=60):
        """
        Проверяет работоспособность кластера через KV операции записи/чтения.
        Аналогично check_kikimr_is_operational из test_distconf.py
        С retry логикой для случаев, когда кластер временно недоступен после failover.

        Args:
            table_path: Путь к KV таблице
            tablet_ids: Список tablet IDs для проверки
            step_name: Название шага теста для логирования (например, "after failover", "after rejoin")
            kv_client: KeyValueClient для использования (если None, используется self.cluster.kv_client)
            max_total_timeout_seconds: Максимальное общее время на все попытки (по умолчанию 60 секунд)
        """
        context = f" (step: {step_name})" if step_name else ""
        self.logger.info("=== CHECKING CLUSTER KV OPERATIONS%s ===", context)

        # Используем переданный клиент или дефолтный
        client_to_use = kv_client if kv_client is not None else self.cluster.kv_client

        start_time = time.time()
        retry_delay = 2  # Задержка между попытками
        attempt = 0

        while time.time() - start_time < max_total_timeout_seconds:
            attempt += 1
            try:
                # Проверяем, что tablet_ids не пустой
                assert len(tablet_ids) > 0, \
                    f"[Step: {step_name}] tablet_ids is empty, cannot perform KV operations"

                # Логируем количество tablets для отладки (только при первой попытке)
                if attempt == 1:
                    self.logger.debug(
                        "[Step: %s] Processing %d tablets: %s",
                        step_name, len(tablet_ids), tablet_ids[:5] if len(tablet_ids) > 5 else tablet_ids
                    )

                for partition_id, tablet_id in enumerate(tablet_ids):
                    # Запись
                    try:
                        write_resp = client_to_use.kv_write(
                            table_path, partition_id, "key", self._value_for("key", tablet_id)
                        )
                        assert write_resp.operation.status == StatusIds.SUCCESS, \
                            f"[Step: {step_name}] KV write failed: partition={partition_id}, tablet={tablet_id}, " \
                            f"status={write_resp.operation.status}, table_path={table_path}"
                        self.logger.info("✓ KV write successful for partition %d, tablet %d%s",
                                         partition_id, tablet_id, context)
                    except Exception as e:
                        elapsed_time = time.time() - start_time
                        if elapsed_time + retry_delay < max_total_timeout_seconds:
                            raise  # Пробрасываем исключение для retry
                        else:
                            # Не хватает времени на еще одну попытку
                            raise AssertionError(
                                f"[Step: {step_name}] KV write operation failed (attempt {attempt}, "
                                f"elapsed: {elapsed_time:.1f}s, timeout: {max_total_timeout_seconds}s): "
                                f"partition={partition_id}, tablet={tablet_id}, table_path={table_path}, error={e}"
                            ) from e

                    # Чтение
                    try:
                        expected_value = self._value_for("key", tablet_id)
                        read_resp = client_to_use.kv_read(
                            table_path, partition_id, "key"
                        )
                        assert read_resp.operation.status == StatusIds.SUCCESS, \
                            f"[Step: {step_name}] KV read failed: partition={partition_id}, tablet={tablet_id}, " \
                            f"status={read_resp.operation.status}, table_path={table_path}"

                        if not hasattr(read_resp, 'operation'):
                            raise AssertionError(
                                f"[Step: {step_name}] KV read response does not contain 'operation' field: "
                                f"partition={partition_id}, tablet={tablet_id}, table_path={table_path}"
                            )

                        operation = read_resp.operation
                        if not hasattr(operation, 'result'):
                            raise AssertionError(
                                f"[Step: {step_name}] KV read response operation does not contain 'result' field: "
                                f"partition={partition_id}, tablet={tablet_id}, table_path={table_path}"
                            )

                        result = operation.result
                        if not hasattr(result, 'value'):
                            raise AssertionError(
                                f"[Step: {step_name}] KV read response result does not contain 'value' field: "
                                f"partition={partition_id}, tablet={tablet_id}, table_path={table_path}"
                            )

                        read_value = result.value

                        if isinstance(read_value, bytes):
                            read_value = read_value.decode('utf-8', errors='replace')
                        if isinstance(expected_value, bytes):
                            expected_value = expected_value.decode('utf-8')

                        # Извлекаем ожидаемое значение из строки (может содержать служебные символы)
                        if expected_value in read_value:
                            value_pos = read_value.find(expected_value)
                            read_value = read_value[value_pos:value_pos + len(expected_value)]

                        assert read_value == expected_value, \
                            f"[Step: {step_name}] KV read value mismatch: partition={partition_id}, tablet={tablet_id}, " \
                            f"expected={expected_value!r}, got={read_value!r}, table_path={table_path}"

                        self.logger.info("✓ KV read successful for partition %d, tablet %d%s (value verified)",
                                         partition_id, tablet_id, context)
                    except Exception as e:
                        elapsed_time = time.time() - start_time
                        if elapsed_time + retry_delay < max_total_timeout_seconds:
                            raise  # Пробрасываем исключение для retry
                        else:
                            # Не хватает времени на еще одну попытку
                            raise AssertionError(
                                f"[Step: {step_name}] KV read operation failed (attempt {attempt}, "
                                f"elapsed: {elapsed_time:.1f}s, timeout: {max_total_timeout_seconds}s): "
                                f"partition={partition_id}, tablet={tablet_id}, table_path={table_path}, error={e}"
                            ) from e

                elapsed_time = time.time() - start_time
                self.logger.info("✓ Cluster KV operations are working correctly%s (took %.1fs)", context, elapsed_time)
                return  # Успешно выполнили все операции

            except (AssertionError, Exception) as e:
                elapsed_time = time.time() - start_time
                if elapsed_time + retry_delay < max_total_timeout_seconds:
                    self.logger.warning(
                        "KV operations failed%s (attempt %d, elapsed: %.1fs, timeout: %ds): %s. Retrying in %d seconds...",
                        context, attempt, elapsed_time, max_total_timeout_seconds, e, retry_delay
                    )
                    time.sleep(retry_delay)
                else:
                    self.logger.error(
                        "KV operations failed%s (attempt %d, total time: %.1fs, max timeout: %ds)",
                        context, attempt, elapsed_time, max_total_timeout_seconds
                    )
                    raise AssertionError(
                        f"[Step: {step_name}] KV operations failed after {attempt} attempts "
                        f"(total time: {elapsed_time:.1f}s, max timeout: {max_total_timeout_seconds}s): {e}"
                    ) from e

        # Если вышли из цикла по таймауту
        elapsed_time = time.time() - start_time
        raise AssertionError(
            f"[Step: {step_name}] KV operations failed: timeout exceeded "
            f"(total time: {elapsed_time:.1f}s, max timeout: {max_total_timeout_seconds}s)"
        )

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
        # Шаг 1: Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state")

        # Шаг 1.5: Создаем KV таблицу для проверки работоспособности кластера (нагрузка)
        self.logger.info("=== CREATING KV TABLE FOR OPERATIONAL CHECKS ===")
        table_path = '/Root/test_bridge_operational_primary'
        number_of_tablets = 3
        swagger_client = SwaggerClient(self.cluster.nodes[1].host, self.cluster.nodes[1].mon_port)

        # Проверяем, какие tablets уже есть в таблице ДО создания новых
        # Если таблицы еще нет, get_kv_tablet_ids может вызвать KeyError - обрабатываем это
        try:
            existing_tablet_ids = get_kv_tablet_ids(swagger_client)
        except (KeyError, Exception):
            # Таблицы еще нет или произошла другая ошибка, значит нет существующих tablets
            existing_tablet_ids = []
        self.logger.debug("Existing tablets before creation: %d tablets", len(existing_tablet_ids))

        # Создаем новые tablets
        create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )

        # Получаем список всех tablets ПОСЛЕ создания
        all_tablet_ids_after = get_kv_tablet_ids(swagger_client)

        # Используем только те tablets, которых не было до создания (новые)
        new_tablet_ids = [tid for tid in all_tablet_ids_after if tid not in existing_tablet_ids]
        tablet_ids = new_tablet_ids[:number_of_tablets]  # Берем только нужное количество

        assert len(tablet_ids) == number_of_tablets, \
            f"[Step: creating KV table] Expected {number_of_tablets} new tablets, got {len(tablet_ids)} " \
            f"(existing: {len(existing_tablet_ids)}, total after: {len(all_tablet_ids_after)}). " \
            f"This may indicate leftover tablets from previous test runs."
        self.logger.info("✓ Created KV table %s with %d new tablets (existing: %d, total: %d)",
                         table_path, len(tablet_ids), len(existing_tablet_ids), len(all_tablet_ids_after))

        # Проверяем работоспособность до failover
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="before failover")

        # Шаг 2: Получаем ноды primary pile
        primary_pile_nodes = self._get_nodes_for_pile_name(primary_pile_name)
        assert primary_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for primary pile {primary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"
        self.logger.info(f"Found {len(primary_pile_nodes)} nodes for primary pile {primary_pile_name}")

        # Шаг 3: Останавливаем ноды primary pile (pile сломался)
        self.logger.info("=== STOPPING PRIMARY PILE NODES ===")
        self._stop_pile_nodes(primary_pile_nodes)
        self.logger.info("✓ Primary pile nodes stopped (pile сломался)")

        # Даем время кластеру обнаружить, что ноды недоступны
        time.sleep(5)

        # Шаг 4: Получаем bridge_client для secondary pile для выполнения failover
        secondary_pile_nodes = self._get_nodes_for_pile_name(secondary_pile_name)
        assert secondary_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for secondary pile {secondary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"

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

        # Используем метод failover() - он сам определит, что primary_pile_name был PRIMARY и переключит secondary_pile_name в PRIMARY
        result = secondary_bridge_client.failover(primary_pile_name)
        assert result, \
            f"[Step: executing failover] Failed to execute failover on {primary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in state_before.pile_states]}"
        self.logger.info("✓ Failover command executed (переключил primary на другой pile)")

        # Шаг 6: Проверяем состояние после failover
        # /bridge_list должна вернуть что pile primary_pile_name в disconnected, secondary_pile_name в primary
        self.logger.info("=== CHECKING STATE AFTER FAILOVER ===")
        expected_states_after_failover = {
            primary_pile_name: PileState.DISCONNECTED,
            secondary_pile_name: PileState.PRIMARY
        }
        self.wait_for_cluster_state_with_step(
            secondary_bridge_client,
            expected_states_after_failover,
            step_name="checking state after failover",
            timeout_seconds=self.TIMEOUT_CLUSTER_STATE_SECONDS
        )
        self.logger.info(f"✓ State after failover: {expected_states_after_failover}")

        # Проверяем текущее состояние через get_cluster_state (аналог /bridge_list)
        state_after_failover = self.get_cluster_state(secondary_bridge_client)
        actual_states = {s.pile_name: s.state for s in state_after_failover.pile_states}
        self.logger.info(f"Bridge list state: {actual_states}")

        assert actual_states.get(primary_pile_name) == PileState.DISCONNECTED, \
            f"[Step: checking state after failover] Expected {primary_pile_name} to be DISCONNECTED, got {actual_states.get(primary_pile_name)}. " \
            f"Full state: {actual_states}"
        assert actual_states.get(secondary_pile_name) == PileState.PRIMARY, \
            f"[Step: checking state after failover] Expected {secondary_pile_name} to be PRIMARY, got {actual_states.get(secondary_pile_name)}. " \
            f"Full state: {actual_states}"

        # Шаг 7: Проверяем, что кластер все еще работает
        self.logger.info("=== CHECKING CLUSTER AVAILABILITY ===")
        try:
            self.get_cluster_state(secondary_bridge_client)
            self.logger.info("✓ Cluster is still accessible and working")
        except Exception as e:
            pytest.fail(
                f"[Step: checking cluster availability after failover] "
                f"Cluster is not accessible: {e}. "
                f"Expected state: {primary_pile_name}=DISCONNECTED, {secondary_pile_name}=PRIMARY"
            )

        # Шаг 7.5: Проверяем работоспособность через KV операции после failover
        # (ошибки связанные с нагрузкой должны пропасть, кластер работает)
        # Создаем новый kv_client к ноде из нового primary pile
        new_primary_kv_client = keyvalue_client_factory(
            secondary_pile_nodes[0].host,
            secondary_pile_nodes[0].grpc_port,
            cluster=self.cluster,
            retry_count=10
        )
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="after failover", kv_client=new_primary_kv_client)

        # Шаг 8: Восстанавливаем ноды primary pile (чиним что сломали)
        self.logger.info("=== RESTORING PRIMARY PILE NODES ===")
        self._start_pile_nodes(primary_pile_nodes)
        self.logger.info("✓ Primary pile nodes started")

        # Даем время нодам запуститься
        time.sleep(5)

        # Шаг 9: Вызываем rejoin на сломанный pile
        self.logger.info("=== EXECUTING REJOIN ===")
        current_state_before_rejoin = self.get_cluster_state(secondary_bridge_client)
        result = secondary_bridge_client.rejoin(primary_pile_name)
        assert result, \
            f"[Step: executing rejoin] Failed to execute rejoin on {primary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in current_state_before_rejoin.pile_states]}"
        self.logger.info("✓ Rejoin command executed")

        # Ждем, пока primary_pile_name вернется в SYNCHRONIZED
        self.logger.info("=== WAITING FOR REJOIN TO COMPLETE ===")
        expected_states_after_rejoin = {
            primary_pile_name: PileState.SYNCHRONIZED,
            secondary_pile_name: PileState.PRIMARY
        }
        self.wait_for_cluster_state_with_step(
            secondary_bridge_client,
            expected_states_after_rejoin,
            step_name="waiting for rejoin to complete",
            timeout_seconds=self.TIMEOUT_REJOIN_SECONDS
        )
        self.logger.info(f"✓ Rejoin completed: {expected_states_after_rejoin}")

        # Финальная проверка состояния
        final_state = self.get_cluster_state(secondary_bridge_client)
        final_states = {s.pile_name: s.state for s in final_state.pile_states}
        self.logger.info(f"Final bridge list state: {final_states}")

        assert final_states.get(primary_pile_name) == PileState.SYNCHRONIZED, \
            f"[Step: checking final state after rejoin] Expected {primary_pile_name} to be SYNCHRONIZED, got {final_states.get(primary_pile_name)}. " \
            f"Full state: {final_states}"
        assert final_states.get(secondary_pile_name) == PileState.PRIMARY, \
            f"[Step: checking final state after rejoin] Expected {secondary_pile_name} to remain PRIMARY, got {final_states.get(secondary_pile_name)}. " \
            f"Full state: {final_states}"

        # Финальная проверка работоспособности через KV операции после rejoin
        # (проверяем что после починки кластер работает, все наши проверки в т.ч. kv)
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="after rejoin")

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
        # Шаг 1: Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state")

        # Шаг 1.5: Создаем KV таблицу для проверки работоспособности кластера (нагрузка)
        self.logger.info("=== CREATING KV TABLE FOR OPERATIONAL CHECKS ===")
        table_path = '/Root/test_bridge_operational_synchronized'
        number_of_tablets = 3
        swagger_client = SwaggerClient(self.cluster.nodes[1].host, self.cluster.nodes[1].mon_port)

        # Проверяем, какие tablets уже есть в таблице ДО создания новых
        # Если таблицы еще нет, get_kv_tablet_ids может вызвать KeyError - обрабатываем это
        try:
            existing_tablet_ids = get_kv_tablet_ids(swagger_client)
        except (KeyError, Exception):
            # Таблицы еще нет или произошла другая ошибка, значит нет существующих tablets
            existing_tablet_ids = []
        self.logger.debug("Existing tablets before creation: %d tablets", len(existing_tablet_ids))

        # Создаем новые tablets
        create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )

        # Получаем список всех tablets ПОСЛЕ создания
        all_tablet_ids_after = get_kv_tablet_ids(swagger_client)

        # Используем только те tablets, которых не было до создания (новые)
        new_tablet_ids = [tid for tid in all_tablet_ids_after if tid not in existing_tablet_ids]
        tablet_ids = new_tablet_ids[:number_of_tablets]  # Берем только нужное количество

        assert len(tablet_ids) == number_of_tablets, \
            f"[Step: creating KV table] Expected {number_of_tablets} new tablets, got {len(tablet_ids)} " \
            f"(existing: {len(existing_tablet_ids)}, total after: {len(all_tablet_ids_after)}). " \
            f"This may indicate leftover tablets from previous test runs."
        self.logger.info("✓ Created KV table %s with %d new tablets (existing: %d, total: %d)",
                         table_path, len(tablet_ids), len(existing_tablet_ids), len(all_tablet_ids_after))

        # Проверяем работоспособность до failover
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="before failover")

        # Шаг 2: Получаем ноды synchronized pile
        synchronized_pile_nodes = self._get_nodes_for_pile_name(secondary_pile_name)
        assert synchronized_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for synchronized pile {secondary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"
        self.logger.info(f"Found {len(synchronized_pile_nodes)} nodes for synchronized pile {secondary_pile_name}")

        # Шаг 3: Останавливаем ноды synchronized pile (pile сломался)
        self.logger.info("=== STOPPING SYNCHRONIZED PILE NODES ===")
        self._stop_pile_nodes(synchronized_pile_nodes)
        self.logger.info("✓ Synchronized pile nodes stopped (pile сломался)")

        # Даем время кластеру обнаружить, что ноды недоступны
        time.sleep(5)

        # Шаг 4: Получаем bridge_client для primary pile для выполнения failover
        primary_pile_nodes = self._get_nodes_for_pile_name(primary_pile_name)
        assert primary_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for primary pile {primary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"

        primary_bridge_client = bridge_client_factory(
            primary_pile_nodes[0].host,
            primary_pile_nodes[0].port,
            cluster=self.cluster,
            retry_count=3,
            timeout=5
        )
        primary_bridge_client.set_auth_token('root@builtin')

        # Шаг 5: Делаем failover на сломанный pile
        # failover() переведет secondary_pile_name в DISCONNECTED, но primary_pile_name останется PRIMARY (так как secondary_pile_name не был PRIMARY)
        self.logger.info("=== EXECUTING FAILOVER ===")
        self.logger.info("Current bridge state before failover:")
        state_before = self.get_cluster_state(primary_bridge_client)
        self.logger.info(f"State: {[(s.pile_name, s.state) for s in state_before.pile_states]}")

        # Используем метод failover() - он переведет secondary_pile_name в DISCONNECTED, но не изменит PRIMARY
        result = primary_bridge_client.failover(secondary_pile_name)
        assert result, \
            f"[Step: executing failover] Failed to execute failover on {secondary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in state_before.pile_states]}"
        self.logger.info("✓ Failover command executed")

        # Шаг 6: Проверяем состояние после failover
        # /bridge_list должна вернуть что pile secondary_pile_name в disconnected, primary_pile_name остается primary
        self.logger.info("=== CHECKING STATE AFTER FAILOVER ===")
        expected_states_after_failover = {
            primary_pile_name: PileState.PRIMARY,
            secondary_pile_name: PileState.DISCONNECTED
        }
        self.wait_for_cluster_state_with_step(
            primary_bridge_client,
            expected_states_after_failover,
            step_name="checking state after failover",
            timeout_seconds=self.TIMEOUT_CLUSTER_STATE_SECONDS
        )
        self.logger.info(f"✓ State after failover: {expected_states_after_failover}")

        # Проверяем текущее состояние через get_cluster_state (аналог /bridge_list)
        state_after_failover = self.get_cluster_state(primary_bridge_client)
        actual_states = {s.pile_name: s.state for s in state_after_failover.pile_states}
        self.logger.info(f"Bridge list state: {actual_states}")

        assert actual_states.get(primary_pile_name) == PileState.PRIMARY, \
            f"[Step: checking state after failover] Expected {primary_pile_name} to remain PRIMARY, got {actual_states.get(primary_pile_name)}. " \
            f"Full state: {actual_states}"
        assert actual_states.get(secondary_pile_name) == PileState.DISCONNECTED, \
            f"[Step: checking state after failover] Expected {secondary_pile_name} to be DISCONNECTED, got {actual_states.get(secondary_pile_name)}. " \
            f"Full state: {actual_states}"

        # Шаг 7: Проверяем, что кластер все еще работает
        self.logger.info("=== CHECKING CLUSTER AVAILABILITY ===")
        try:
            self.get_cluster_state(primary_bridge_client)
            self.logger.info("✓ Cluster is still accessible and working")
        except Exception as e:
            pytest.fail(
                f"[Step: checking cluster availability after failover] "
                f"Cluster is not accessible: {e}. "
                f"Expected state: {primary_pile_name}=PRIMARY, {secondary_pile_name}=DISCONNECTED"
            )

        # Шаг 7.5: Проверяем работоспособность через KV операции после failover
        # (ошибки связанные с нагрузкой должны пропасть, кластер работает)
        # В этом сценарии primary_pile_name остается PRIMARY, но создаем новый kv_client
        # к ноде из primary pile для надежности
        primary_kv_client = keyvalue_client_factory(
            primary_pile_nodes[0].host,
            primary_pile_nodes[0].grpc_port,
            cluster=self.cluster,
            retry_count=10
        )
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="after failover", kv_client=primary_kv_client)

        # Шаг 8: Восстанавливаем ноды synchronized pile (чиним что сломали)
        self.logger.info("=== RESTORING SYNCHRONIZED PILE NODES ===")
        self._start_pile_nodes(synchronized_pile_nodes)
        self.logger.info("✓ Synchronized pile nodes started")

        # Даем время нодам запуститься
        time.sleep(5)

        # Шаг 9: Вызываем rejoin на сломанный pile
        self.logger.info("=== EXECUTING REJOIN ===")
        current_state_before_rejoin = self.get_cluster_state(primary_bridge_client)
        result = primary_bridge_client.rejoin(secondary_pile_name)
        assert result, \
            f"[Step: executing rejoin] Failed to execute rejoin on {secondary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in current_state_before_rejoin.pile_states]}"
        self.logger.info("✓ Rejoin command executed")

        # Ждем, пока secondary_pile_name вернется в SYNCHRONIZED
        self.logger.info("=== WAITING FOR REJOIN TO COMPLETE ===")
        expected_states_after_rejoin = {
            primary_pile_name: PileState.PRIMARY,
            secondary_pile_name: PileState.SYNCHRONIZED
        }
        self.wait_for_cluster_state_with_step(
            primary_bridge_client,
            expected_states_after_rejoin,
            step_name="waiting for rejoin to complete",
            timeout_seconds=self.TIMEOUT_REJOIN_SECONDS
        )
        self.logger.info(f"✓ Rejoin completed: {expected_states_after_rejoin}")

        # Финальная проверка состояния
        final_state = self.get_cluster_state(primary_bridge_client)
        final_states = {s.pile_name: s.state for s in final_state.pile_states}
        self.logger.info(f"Final bridge list state: {final_states}")

        assert final_states.get(primary_pile_name) == PileState.PRIMARY, \
            f"[Step: checking final state after rejoin] Expected {primary_pile_name} to remain PRIMARY, got {final_states.get(primary_pile_name)}. " \
            f"Full state: {final_states}"
        assert final_states.get(secondary_pile_name) == PileState.SYNCHRONIZED, \
            f"[Step: checking final state after rejoin] Expected {secondary_pile_name} to be SYNCHRONIZED, got {final_states.get(secondary_pile_name)}. " \
            f"Full state: {final_states}"

        # Финальная проверка работоспособности через KV операции после rejoin
        # (проверяем что после починки кластер работает, все наши проверки в т.ч. kv)
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="after rejoin")

        self.logger.info("✓ Test completed successfully")

    def test_failover_with_partial_primary_pile_node_stop(self):
        """
        Сценарий 3: Останавливаем только часть нод Primary pile, делаем failover, восстанавливаем.

        Шаги сценария:
        1. Определяем текущее состояние кластера (независимо от конкретных имен piles)
        2. Создаем KV таблицу для проверки работоспособности (нагрузка на кластер)
        3. Проверяем работоспособность до failover через KV операции
        4. Останавливаем только часть нод primary pile - частичный сбой
           - Останавливаем примерно половину нод или минимум одну
        5. Делаем failover на сломанный pile
           - failover() определяет, что primary pile был PRIMARY и переключает secondary в PRIMARY
           - primary pile переходит в DISCONNECTED
        6. Проверяем состояние после failover через /bridge_list (get_cluster_state)
           - Должны получить: primary=DISCONNECTED, secondary=PRIMARY
        7. Проверяем работоспособность через KV операции после failover
        8. Восстанавливаем остановленные ноды
        9. Вызываем rejoin на сломанный pile
        10. Проверяем финальное состояние: primary=SYNCHRONIZED, secondary=PRIMARY
        11. Проверяем работоспособность через KV операции после rejoin
        """
        # Шаг 1: Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("determining current cluster state")

        # Шаг 2: Создаем KV таблицу для проверки работоспособности
        self.logger.info("=== CREATING KV TABLE FOR OPERATIONAL CHECKS ===")
        table_path = '/Root/test_bridge_operational_partial'
        number_of_tablets = 3
        swagger_client = SwaggerClient(self.cluster.nodes[1].host, self.cluster.nodes[1].mon_port)

        # Проверяем, какие tablets уже есть в таблице ДО создания новых
        # Если таблицы еще нет, get_kv_tablet_ids может вызвать KeyError - обрабатываем это
        try:
            existing_tablet_ids = get_kv_tablet_ids(swagger_client)
        except (KeyError, Exception):
            # Таблицы еще нет или произошла другая ошибка, значит нет существующих tablets
            existing_tablet_ids = []
        self.logger.debug("Existing tablets before creation: %d tablets", len(existing_tablet_ids))

        # Создаем новые tablets
        create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )

        # Получаем список всех tablets ПОСЛЕ создания
        all_tablet_ids_after = get_kv_tablet_ids(swagger_client)

        # Используем только те tablets, которых не было до создания (новые)
        new_tablet_ids = [tid for tid in all_tablet_ids_after if tid not in existing_tablet_ids]
        tablet_ids = new_tablet_ids[:number_of_tablets]  # Берем только нужное количество

        assert len(tablet_ids) == number_of_tablets, \
            f"[Step: creating KV table] Expected {number_of_tablets} new tablets, got {len(tablet_ids)} " \
            f"(existing: {len(existing_tablet_ids)}, total after: {len(all_tablet_ids_after)}). " \
            f"This may indicate leftover tablets from previous test runs."
        self.logger.info("✓ Created KV table %s with %d new tablets (existing: %d, total: %d)",
                         table_path, len(tablet_ids), len(existing_tablet_ids), len(all_tablet_ids_after))

        # Проверяем работоспособность до failover
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="before failover")

        # Шаг 3: Получаем ноды primary pile
        primary_pile_nodes = self._get_nodes_for_pile_name(primary_pile_name)
        assert primary_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for primary pile {primary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"
        self.logger.info(f"Found {len(primary_pile_nodes)} nodes for primary pile {primary_pile_name}")

        # Шаг 4: Останавливаем только часть нод (примерно половину или минимум одну)
        nodes_to_stop_count = max(1, len(primary_pile_nodes) // 2)
        nodes_to_stop = primary_pile_nodes[:nodes_to_stop_count]

        self.logger.info("=== STOPPING PARTIAL PRIMARY PILE NODES ===")
        self.logger.info(f"Stopping {len(nodes_to_stop)} out of {len(primary_pile_nodes)} nodes")
        self._stop_pile_nodes(nodes_to_stop)
        self.logger.info("✓ Partial primary pile nodes stopped")

        # Даем время кластеру обнаружить частичный сбой
        time.sleep(5)

        # Шаг 5: Получаем bridge_client для secondary pile для выполнения failover
        secondary_pile_nodes = self._get_nodes_for_pile_name(secondary_pile_name)
        assert secondary_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for secondary pile {secondary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"

        secondary_bridge_client = bridge_client_factory(
            secondary_pile_nodes[0].host,
            secondary_pile_nodes[0].port,
            cluster=self.cluster,
            retry_count=3,
            timeout=5
        )
        secondary_bridge_client.set_auth_token('root@builtin')

        # Делаем failover на сломанный pile
        self.logger.info("=== EXECUTING FAILOVER ===")
        state_before_failover = self.get_cluster_state(secondary_bridge_client)
        result = secondary_bridge_client.failover(primary_pile_name)
        assert result, \
            f"[Step: executing failover] Failed to execute failover on {primary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in state_before_failover.pile_states]}"
        self.logger.info("✓ Failover command executed")

        # Шаг 6: Проверяем состояние после failover
        self.logger.info("=== CHECKING STATE AFTER FAILOVER ===")
        expected_states_after_failover = {
            primary_pile_name: PileState.DISCONNECTED,
            secondary_pile_name: PileState.PRIMARY
        }
        self.wait_for_cluster_state_with_step(
            secondary_bridge_client,
            expected_states_after_failover,
            step_name="checking state after failover",
            timeout_seconds=self.TIMEOUT_CLUSTER_STATE_SECONDS
        )
        self.logger.info(f"✓ State after failover: {expected_states_after_failover}")

        state_after_failover = self.get_cluster_state(secondary_bridge_client)
        actual_states = {s.pile_name: s.state for s in state_after_failover.pile_states}
        self.logger.info(f"Bridge list state: {actual_states}")

        assert actual_states.get(primary_pile_name) == PileState.DISCONNECTED, \
            f"[Step: checking state after failover] Expected {primary_pile_name} to be DISCONNECTED, got {actual_states.get(primary_pile_name)}. " \
            f"Full state: {actual_states}"
        assert actual_states.get(secondary_pile_name) == PileState.PRIMARY, \
            f"[Step: checking state after failover] Expected {secondary_pile_name} to be PRIMARY, got {actual_states.get(secondary_pile_name)}. " \
            f"Full state: {actual_states}"

        # Шаг 7: Проверяем работоспособность через KV операции после failover
        # Создаем новый kv_client к ноде из нового primary pile
        new_primary_kv_client = keyvalue_client_factory(
            secondary_pile_nodes[0].host,
            secondary_pile_nodes[0].grpc_port,
            cluster=self.cluster,
            retry_count=10
        )
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="after failover", kv_client=new_primary_kv_client)

        # Шаг 8: Восстанавливаем остановленные ноды
        self.logger.info("=== RESTORING STOPPED NODES ===")
        self._start_pile_nodes(nodes_to_stop)
        self.logger.info("✓ Stopped nodes restored")

        # Даем время нодам запуститься
        time.sleep(5)

        # Шаг 9: Вызываем rejoin
        self.logger.info("=== EXECUTING REJOIN ===")
        current_state_before_rejoin = self.get_cluster_state(secondary_bridge_client)
        result = secondary_bridge_client.rejoin(primary_pile_name)
        assert result, \
            f"[Step: executing rejoin] Failed to execute rejoin on {primary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in current_state_before_rejoin.pile_states]}"
        self.logger.info("✓ Rejoin command executed")

        # Ждем, пока primary_pile_name вернется в SYNCHRONIZED
        self.logger.info("=== WAITING FOR REJOIN TO COMPLETE ===")
        expected_states_after_rejoin = {
            primary_pile_name: PileState.SYNCHRONIZED,
            secondary_pile_name: PileState.PRIMARY
        }
        self.wait_for_cluster_state_with_step(
            secondary_bridge_client,
            expected_states_after_rejoin,
            step_name="waiting for rejoin to complete",
            timeout_seconds=self.TIMEOUT_REJOIN_SECONDS
        )
        self.logger.info(f"✓ Rejoin completed: {expected_states_after_rejoin}")

        # Шаг 10: Финальная проверка состояния
        final_state = self.get_cluster_state(secondary_bridge_client)
        final_states = {s.pile_name: s.state for s in final_state.pile_states}
        self.logger.info(f"Final bridge list state: {final_states}")

        assert final_states.get(primary_pile_name) == PileState.SYNCHRONIZED, \
            f"[Step: checking final state after rejoin] Expected {primary_pile_name} to be SYNCHRONIZED, got {final_states.get(primary_pile_name)}. " \
            f"Full state: {final_states}"
        assert final_states.get(secondary_pile_name) == PileState.PRIMARY, \
            f"[Step: checking final state after rejoin] Expected {secondary_pile_name} to remain PRIMARY, got {final_states.get(secondary_pile_name)}. " \
            f"Full state: {final_states}"

        # Шаг 11: Финальная проверка работоспособности через KV операции
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="after rejoin")

        self.logger.info("✓ Test completed successfully")

    def test_multiple_failover_rejoin_cycles(self):
        """
        Сценарий 4: Выполняем несколько циклов failover/rejoin подряд.
        Проверяем стабильность системы при повторяющихся переключениях.

        Шаги сценария:
        1. Проверяем начальное состояние кластера (r1=PRIMARY, r2=SYNCHRONIZED)
        2. Создаем KV таблицу для проверки работоспособности
        3. Выполняем несколько циклов (2 цикла):
           - Останавливаем primary pile
           - Делаем failover
           - Проверяем работоспособность
           - Восстанавливаем ноды
           - Делаем rejoin
           - Проверяем работоспособность
        4. Проверяем финальное состояние и работоспособность
        """
        # Шаг 1: Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state")

        # Шаг 2: Создаем KV таблицу
        self.logger.info("=== CREATING KV TABLE FOR OPERATIONAL CHECKS ===")
        table_path = '/Root/test_bridge_operational_cycles'
        number_of_tablets = 3
        swagger_client = SwaggerClient(self.cluster.nodes[1].host, self.cluster.nodes[1].mon_port)

        # Проверяем, какие tablets уже есть в таблице ДО создания новых
        # Если таблицы еще нет, get_kv_tablet_ids может вызвать KeyError - обрабатываем это
        try:
            existing_tablet_ids = get_kv_tablet_ids(swagger_client)
        except (KeyError, Exception):
            # Таблицы еще нет или произошла другая ошибка, значит нет существующих tablets
            existing_tablet_ids = []
        self.logger.debug("Existing tablets before creation: %d tablets", len(existing_tablet_ids))

        # Создаем новые tablets
        create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )

        # Получаем список всех tablets ПОСЛЕ создания
        all_tablet_ids_after = get_kv_tablet_ids(swagger_client)

        # Используем только те tablets, которых не было до создания (новые)
        new_tablet_ids = [tid for tid in all_tablet_ids_after if tid not in existing_tablet_ids]
        tablet_ids = new_tablet_ids[:number_of_tablets]  # Берем только нужное количество

        assert len(tablet_ids) == number_of_tablets, \
            f"[Step: creating KV table] Expected {number_of_tablets} new tablets, got {len(tablet_ids)} " \
            f"(existing: {len(existing_tablet_ids)}, total after: {len(all_tablet_ids_after)}). " \
            f"This may indicate leftover tablets from previous test runs."
        self.logger.info("✓ Created KV table %s with %d new tablets (existing: %d, total: %d)",
                         table_path, len(tablet_ids), len(existing_tablet_ids), len(all_tablet_ids_after))

        # Проверяем работоспособность до циклов
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="before cycles")

        # Шаг 3: Выполняем несколько циклов failover/rejoin
        cycles_count = 2
        for cycle in range(cycles_count):
            self.logger.info(f"=== CYCLE {cycle + 1}/{cycles_count} ===")

            # Определяем какой pile сейчас PRIMARY (динамически, независимо от предыдущего состояния)
            primary_pile_name, secondary_pile_name = self._determine_current_cluster_state(f"cycle {cycle + 1}, determining current state")

            # Получаем ноды primary pile
            primary_pile_nodes = self._get_nodes_for_pile_name(primary_pile_name)
            assert primary_pile_nodes, \
                f"[Step: cycle {cycle + 1}, getting nodes] Could not find nodes for primary pile {primary_pile_name}. " \
                f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"

            # Останавливаем primary pile
            self.logger.info(f"=== STOPPING {primary_pile_name.upper()} PILE NODES ===")
            self._stop_pile_nodes(primary_pile_nodes)
            time.sleep(5)

            # Получаем bridge_client для secondary pile
            secondary_pile_nodes = self._get_nodes_for_pile_name(secondary_pile_name)
            assert secondary_pile_nodes, \
                f"[Step: cycle {cycle + 1}, getting nodes] Could not find nodes for secondary pile {secondary_pile_name}. " \
                f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"

            bridge_client = bridge_client_factory(
                secondary_pile_nodes[0].host,
                secondary_pile_nodes[0].port,
                cluster=self.cluster,
                retry_count=3,
                timeout=5
            )
            bridge_client.set_auth_token('root@builtin')

            # Делаем failover
            self.logger.info(f"=== EXECUTING FAILOVER ON {primary_pile_name.upper()} ===")
            state_before_failover = self.get_cluster_state(bridge_client)
            result = bridge_client.failover(primary_pile_name)
            assert result, \
                f"[Step: cycle {cycle + 1}, executing failover] Failed to execute failover on {primary_pile_name}. " \
                f"Current state: {[(s.pile_name, s.state) for s in state_before_failover.pile_states]}"
            self.logger.info("✓ Failover executed")

            # Проверяем состояние после failover
            expected_states = {
                primary_pile_name: PileState.DISCONNECTED,
                secondary_pile_name: PileState.PRIMARY
            }
            self.wait_for_cluster_state_with_step(
                bridge_client,
                expected_states,
                step_name=f"cycle {cycle + 1}, checking state after failover",
                timeout_seconds=self.TIMEOUT_CLUSTER_STATE_SECONDS
            )
            self.logger.info(f"✓ State after failover: {expected_states}")

            # Проверяем работоспособность
            # Создаем новый kv_client к ноде из нового primary pile
            new_primary_kv_client = keyvalue_client_factory(
                secondary_pile_nodes[0].host,
                secondary_pile_nodes[0].grpc_port,
                cluster=self.cluster,
                retry_count=10
            )
            self._check_cluster_kv_operations(table_path, tablet_ids, step_name=f"cycle {cycle + 1} after failover", kv_client=new_primary_kv_client)

            # Восстанавливаем ноды
            self.logger.info(f"=== RESTORING {primary_pile_name.upper()} PILE NODES ===")
            self._start_pile_nodes(primary_pile_nodes)
            time.sleep(5)

            # Делаем rejoin
            self.logger.info(f"=== EXECUTING REJOIN ON {primary_pile_name.upper()} ===")
            current_state_before_rejoin = self.get_cluster_state(bridge_client)
            result = bridge_client.rejoin(primary_pile_name)
            assert result, \
                f"[Step: cycle {cycle + 1}, executing rejoin] Failed to execute rejoin on {primary_pile_name}. " \
                f"Current state: {[(s.pile_name, s.state) for s in current_state_before_rejoin.pile_states]}"

            # Ждем завершения rejoin
            expected_states_after_rejoin = {
                primary_pile_name: PileState.SYNCHRONIZED,
                secondary_pile_name: PileState.PRIMARY
            }
            self.wait_for_cluster_state_with_step(
                bridge_client,
                expected_states_after_rejoin,
                step_name=f"cycle {cycle + 1}, waiting for rejoin to complete",
                timeout_seconds=self.TIMEOUT_REJOIN_SECONDS
            )
            self.logger.info(f"✓ Rejoin completed: {expected_states_after_rejoin}")

            # Проверяем работоспособность после rejoin
            self._check_cluster_kv_operations(table_path, tablet_ids, step_name=f"cycle {cycle + 1} after rejoin")
            self.logger.info(f"✓ Cycle {cycle + 1} completed successfully")

        # Шаг 4: Финальная проверка
        final_state = self.get_cluster_state(self.bridge_client)
        final_states = {s.pile_name: s.state for s in final_state.pile_states}
        self.logger.info(f"Final bridge list state: {final_states}")

        # После циклов должен быть один PRIMARY и один SYNCHRONIZED
        primary_count = sum(1 for state in final_states.values() if state == PileState.PRIMARY)
        synchronized_count = sum(1 for state in final_states.values() if state == PileState.SYNCHRONIZED)
        assert primary_count == 1, \
            f"[Step: checking final state after cycles] Expected exactly one PRIMARY, got {primary_count}. " \
            f"Full state: {final_states}"
        assert synchronized_count == 1, \
            f"[Step: checking final state after cycles] Expected exactly one SYNCHRONIZED, got {synchronized_count}. " \
            f"Full state: {final_states}"

        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="final after cycles")
        self.logger.info("✓ Test completed successfully")

    def test_failover_during_active_kv_operations(self):
        """
        Сценарий 5: Выполняем KV операции во время failover.
        Проверяем целостность данных и отсутствие потерь.

        Шаги сценария:
        1. Проверяем начальное состояние кластера (r1=PRIMARY, r2=SYNCHRONIZED)
        2. Создаем KV таблицу и записываем начальные данные
        3. Запускаем фоновые KV операции (запись/чтение)
        4. Во время выполнения операций останавливаем primary pile
        5. Делаем failover
        6. Проверяем, что все данные сохранились и доступны
        7. Продолжаем выполнять KV операции после failover
        8. Восстанавливаем ноды и делаем rejoin
        9. Проверяем финальную целостность данных
        """
        # Шаг 1: Определяем текущее состояние кластера (независимо от конкретных имен piles)
        primary_pile_name, secondary_pile_name = self._determine_current_cluster_state("checking initial state")

        # Шаг 2: Создаем KV таблицу и записываем начальные данные
        self.logger.info("=== CREATING KV TABLE FOR OPERATIONAL CHECKS ===")
        table_path = '/Root/test_bridge_operational_during_failover'
        number_of_tablets = 3
        swagger_client = SwaggerClient(self.cluster.nodes[1].host, self.cluster.nodes[1].mon_port)

        # Проверяем, какие tablets уже есть в таблице ДО создания новых
        # Если таблицы еще нет, get_kv_tablet_ids может вызвать KeyError - обрабатываем это
        try:
            existing_tablet_ids = get_kv_tablet_ids(swagger_client)
        except (KeyError, Exception):
            # Таблицы еще нет или произошла другая ошибка, значит нет существующих tablets
            existing_tablet_ids = []
        self.logger.debug("Existing tablets before creation: %d tablets", len(existing_tablet_ids))

        # Создаем новые tablets
        create_kv_tablets_and_wait_for_start(
            self.cluster.client,
            self.cluster.kv_client,
            swagger_client,
            number_of_tablets,
            table_path,
            timeout_seconds=10
        )

        # Получаем список всех tablets ПОСЛЕ создания
        all_tablet_ids_after = get_kv_tablet_ids(swagger_client)

        # Используем только те tablets, которых не было до создания (новые)
        new_tablet_ids = [tid for tid in all_tablet_ids_after if tid not in existing_tablet_ids]
        tablet_ids = new_tablet_ids[:number_of_tablets]  # Берем только нужное количество

        assert len(tablet_ids) == number_of_tablets, \
            f"[Step: creating KV table] Expected {number_of_tablets} new tablets, got {len(tablet_ids)} " \
            f"(existing: {len(existing_tablet_ids)}, total after: {len(all_tablet_ids_after)}). " \
            f"This may indicate leftover tablets from previous test runs."
        self.logger.info("✓ Created KV table %s with %d new tablets (existing: %d, total: %d)",
                         table_path, len(tablet_ids), len(existing_tablet_ids), len(all_tablet_ids_after))

        # Записываем начальные данные
        initial_data = {}
        for partition_id, tablet_id in enumerate(tablet_ids):
            key = f"initial_key_{partition_id}"
            value = self._value_for(key, tablet_id)
            try:
                write_resp = self.cluster.kv_client.kv_write(table_path, partition_id, key, value)
                assert write_resp.operation.status == StatusIds.SUCCESS, \
                    f"[Step: writing initial data] KV write failed: partition={partition_id}, " \
                    f"key={key}, status={write_resp.operation.status}, table_path={table_path}"
                initial_data[partition_id] = (key, value)
            except Exception as e:
                raise AssertionError(
                    f"[Step: writing initial data] Failed to write initial data: "
                    f"partition={partition_id}, key={key}, table_path={table_path}, error={e}"
                ) from e
        self.logger.info("✓ Initial data written")

        # Шаг 3: Запускаем фоновые KV операции
        operations_completed = threading.Event()
        operations_errors = []
        operations_count = {'write': 0, 'read': 0}

        def background_kv_operations():
            """Выполняет KV операции в фоне"""
            try:
                iteration = 0
                while not operations_completed.is_set() and iteration < 50:
                    for partition_id, tablet_id in enumerate(tablet_ids):
                        # Запись
                        key = f"bg_key_{partition_id}_{iteration}"
                        value = self._value_for(key, tablet_id)
                        try:
                            write_resp = self.cluster.kv_client.kv_write(
                                table_path, partition_id, key, value
                            )
                            if write_resp.operation.status == StatusIds.SUCCESS:
                                operations_count['write'] += 1
                        except Exception as e:
                            operations_errors.append(f"Write error at iteration {iteration}: {e}")

                        # Чтение начальных данных
                        try:
                            read_key, _ = initial_data[partition_id]
                            read_resp = self.cluster.kv_client.kv_read(
                                table_path, partition_id, read_key
                            )
                            if read_resp.operation.status == StatusIds.SUCCESS:
                                operations_count['read'] += 1
                        except Exception as e:
                            operations_errors.append(f"Read error at iteration {iteration}: {e}")

                    iteration += 1
                    time.sleep(0.5)
            except Exception as e:
                operations_errors.append(f"Background operations error: {e}")

        bg_thread = threading.Thread(target=background_kv_operations, daemon=True)
        bg_thread.start()
        self.logger.info("✓ Background KV operations started")

        # Даем время операциям начаться
        time.sleep(2)

        # Шаг 4: Останавливаем primary pile во время операций
        primary_pile_nodes = self._get_nodes_for_pile_name(primary_pile_name)
        assert primary_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for primary pile {primary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"

        self.logger.info("=== STOPPING PRIMARY PILE NODES DURING OPERATIONS ===")
        self._stop_pile_nodes(primary_pile_nodes)
        time.sleep(5)

        # Шаг 5: Делаем failover
        secondary_pile_nodes = self._get_nodes_for_pile_name(secondary_pile_name)
        assert secondary_pile_nodes, \
            f"[Step: getting nodes] Could not find nodes for secondary pile {secondary_pile_name}. " \
            f"Available nodes: {[n.node_id for n in self.cluster.nodes.values()]}"

        secondary_bridge_client = bridge_client_factory(
            secondary_pile_nodes[0].host,
            secondary_pile_nodes[0].port,
            cluster=self.cluster,
            retry_count=3,
            timeout=5
        )
        secondary_bridge_client.set_auth_token('root@builtin')

        self.logger.info("=== EXECUTING FAILOVER DURING OPERATIONS ===")
        state_before_failover = self.get_cluster_state(secondary_bridge_client)
        result = secondary_bridge_client.failover(primary_pile_name)
        assert result, \
            f"[Step: executing failover during operations] Failed to execute failover on {primary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in state_before_failover.pile_states]}"
        self.logger.info("✓ Failover executed")

        # Даем время операциям продолжиться после failover
        time.sleep(3)

        # Останавливаем фоновые операции
        operations_completed.set()
        bg_thread.join(timeout=10)
        self.logger.info(f"✓ Background operations completed: {operations_count} operations, {len(operations_errors)} errors")

        # Шаг 6: Проверяем, что начальные данные сохранились
        # Создаем новый kv_client к ноде из нового primary pile
        new_primary_kv_client = keyvalue_client_factory(
            secondary_pile_nodes[0].host,
            secondary_pile_nodes[0].grpc_port,
            cluster=self.cluster,
            retry_count=10
        )
        self._check_data_integrity(
            table_path,
            initial_data,
            step_name="checking data integrity after failover",
            kv_client=new_primary_kv_client
        )

        # Шаг 7: Продолжаем выполнять KV операции после failover
        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="after failover during operations", kv_client=new_primary_kv_client)

        # Шаг 8: Восстанавливаем ноды и делаем rejoin
        self.logger.info("=== RESTORING PRIMARY PILE NODES ===")
        self._start_pile_nodes(primary_pile_nodes)
        time.sleep(5)

        self.logger.info("=== EXECUTING REJOIN ===")
        current_state_before_rejoin = self.get_cluster_state(secondary_bridge_client)
        result = secondary_bridge_client.rejoin(primary_pile_name)
        assert result, \
            f"[Step: executing rejoin] Failed to execute rejoin on {primary_pile_name}. " \
            f"Current state: {[(s.pile_name, s.state) for s in current_state_before_rejoin.pile_states]}"

        expected_states_after_rejoin = {
            primary_pile_name: PileState.SYNCHRONIZED,
            secondary_pile_name: PileState.PRIMARY
        }
        self.wait_for_cluster_state_with_step(
            secondary_bridge_client,
            expected_states_after_rejoin,
            step_name="waiting for rejoin to complete",
            timeout_seconds=self.TIMEOUT_REJOIN_SECONDS
        )
        self.logger.info(f"✓ Rejoin completed: {expected_states_after_rejoin}")

        # Шаг 9: Финальная проверка целостности данных
        self._check_data_integrity(
            table_path,
            initial_data,
            step_name="final data integrity check"
        )

        self._check_cluster_kv_operations(table_path, tablet_ids, step_name="final after rejoin")

        # Логируем статистику операций
        self.logger.info(f"Background operations summary: {operations_count['write']} writes, {operations_count['read']} reads")
        if operations_errors:
            self.logger.warning(f"Operations errors (expected during failover): {len(operations_errors)} errors")

        self.logger.info("✓ Test completed successfully")
