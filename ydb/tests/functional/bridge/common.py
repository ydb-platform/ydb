from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState

from hamcrest import assert_that, is_, has_entries

import logging
import time


class BridgeKiKiMRTest(object):
    erasure = Erasure.BLOCK_4_2
    use_config_store = True
    separate_node_configs = True
    nodes_count = 16
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }
    logger = logging.getLogger(__name__)

    @classmethod
    def setup_class(cls):
        log_configs = {
            'GRPC_SERVER': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.DEBUG,
        }

        bridge_config = {
            "piles": [
                {"name": "r1"},
                {"name": "r2"}
            ]
        }

        cls.configurator = KikimrConfigGenerator(
            cls.erasure,
            nodes=cls.nodes_count,
            use_in_memory_pdisks=False,
            use_config_store=cls.use_config_store,
            metadata_section=cls.metadata_section,
            separate_node_configs=cls.separate_node_configs,
            simple_config=True,
            use_self_management=True,
            extra_grpc_services=['bridge'],
            additional_log_configs=log_configs,
            bridge_config=bridge_config
        )

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

        host = cls.cluster.nodes[1].host
        grpc_port = cls.cluster.nodes[1].port
        cls.bridge_client = BridgeClient(host, grpc_port)
        cls.secondary_bridge_client = BridgeClient(cls.cluster.nodes[2].host, cls.cluster.nodes[2].port)
        cls.bridge_client.set_auth_token('root@builtin')

    @classmethod
    def teardown_class(cls):
        cls.bridge_client.close()
        cls.cluster.stop()

    def update_cluster_state(self, client, updates, expected_status=StatusIds.SUCCESS):
        response = client.update_cluster_state(updates)
        self.logger.debug("Update cluster state response: %s", response)
        assert_that(response.operation.status, is_(expected_status))
        if expected_status == StatusIds.SUCCESS:
            result = bridge.UpdateClusterStateResult()
            response.operation.result.Unpack(result)
            return result
        else:
            return response

    def get_cluster_state(self, client):
        response = client.get_cluster_state()
        assert_that(response.operation.status, is_(StatusIds.SUCCESS))
        result = bridge.GetClusterStateResult()
        response.operation.result.Unpack(result)
        self.logger.debug("Get cluster state result: %s", result)
        return result

    def get_cluster_state_and_check(self, client, expected_states):
        result = self.get_cluster_state(client)
        actual_states = {s.pile_name: s.state for s in result.pile_states}
        assert_that(actual_states, is_(has_entries(expected_states)))
        assert_that(len(actual_states), is_(len(expected_states)))
        return result

    def wait_for_cluster_state(self, client, expected_states, timeout_seconds=30):
        start_time = time.time()
        last_exception = None

        while time.time() - start_time < timeout_seconds:
            try:
                self.get_cluster_state_and_check(client, expected_states)
                return
            except (AssertionError, Exception) as e:
                last_exception = e
                time.sleep(0.5)

        # Пытаемся получить текущее состояние для более информативного сообщения об ошибке
        current_states = None
        get_state_error_msg = None
        try:
            current_result = self.get_cluster_state(client)
            current_states = {s.pile_name: s.state for s in current_result.pile_states}
        except Exception as get_state_error:
            get_state_error_msg = str(get_state_error)

        expected_str = ", ".join([f"{k}={v}" for k, v in expected_states.items()])
        if current_states:
            current_str = ", ".join([f"{k}={v}" for k, v in current_states.items()])
        else:
            error_info = f" (error: {get_state_error_msg})" if get_state_error_msg else ""
            current_str = f"unavailable{error_info}"

        raise AssertionError(
            f"Cluster state did not reach expected state in {timeout_seconds}s. "
            f"Expected: {expected_str}. Current state: {current_str}"
        ) from last_exception

    def wait_for_cluster_state_with_step(self, client, expected_states, step_name, timeout_seconds=30):
        """
        Обертка над wait_for_cluster_state с указанием шага теста для понятных assert-сообщений.

        Args:
            client: BridgeClient для получения состояния
            expected_states: Словарь ожидаемых состояний {pile_name: PileState}
            step_name: Название шага теста (например, "checking state after failover")
            timeout_seconds: Максимальное время ожидания (ограничено максимумом 60 секунд)
        """
        # Ограничиваем таймаут максимумом 60 секунд
        timeout_seconds = min(timeout_seconds, 60)
        try:
            return self.wait_for_cluster_state(client, expected_states, timeout_seconds=timeout_seconds)
        except AssertionError as e:
            # Извлекаем информацию из оригинального сообщения
            original_msg = str(e)

            # Извлекаем количество попыток и время
            attempts_info = ""
            time_info = ""
            if "after" in original_msg and "attempts" in original_msg:
                # Извлекаем "X attempts"
                attempts_part = original_msg.split("after")[1].split("(")[0].strip()
                attempts_info = f" {attempts_part}"

            if "total time:" in original_msg:
                # Извлекаем информацию о времени
                time_part = original_msg.split("total time:")[1].split(",")[0].strip()
                timeout_part = original_msg.split("timeout:")[1].split(")")[0].strip() if "timeout:" in original_msg else ""
                time_info = f" (total time: {time_part}, timeout: {timeout_part})" if timeout_part else f" (total time: {time_part})"

            # Извлекаем Expected и Current state из оригинального сообщения (если они там есть)
            expected_str = ", ".join([f"{k}={v}" for k, v in expected_states.items()])
            current_str = "unknown"

            if "Expected:" in original_msg and "Current state:" in original_msg:
                # Используем информацию из оригинального сообщения
                expected_part = original_msg.split("Expected:")[1].split(".")[0].strip() if "Expected:" in original_msg else expected_str
                current_part = original_msg.split("Current state:")[1].strip() if "Current state:" in original_msg else "unknown"
                expected_str = expected_part
                current_str = current_part
            else:
                # Получаем текущее состояние сами
                try:
                    current_result = self.get_cluster_state(client)
                    current_states = {s.pile_name: s.state for s in current_result.pile_states}
                    current_str = ", ".join([f"{k}={v}" for k, v in current_states.items()])
                except Exception as get_state_error:
                    current_str = f"unavailable (error: {get_state_error})"

            raise AssertionError(
                f"[Step: {step_name}] Failed to reach expected cluster state{attempts_info}{time_info}. "
                f"Expected: {expected_str}. Current state: {current_str}"
            ) from e

    def _determine_current_cluster_state(self, step_name="determining current cluster state", timeout_seconds=60):
        """
        Определяет текущее состояние кластера и возвращает имена primary и secondary piles.
        Независимо от конкретных имен piles (r1/r2), определяет какой pile сейчас PRIMARY.
        Ждет, пока кластер достигнет стабильного состояния (один PRIMARY, один SYNCHRONIZED).

        Args:
            step_name: Название шага для логирования
            timeout_seconds: Таймаут для ожидания стабильного состояния

        Returns:
            tuple: (primary_pile_name, secondary_pile_name)
        """
        self.logger.info(f"=== {step_name.upper()} ===")

        # Сначала ждем, пока кластер станет доступным (обрабатываем Connection refused)
        start_time = time.time()
        cluster_available = False
        while time.time() - start_time < timeout_seconds:
            try:
                current_state = self.get_cluster_state(self.bridge_client)
                cluster_available = True
                break
            except Exception as e:
                self.logger.debug(f"Cluster not available yet: {e}. Retrying...")
                time.sleep(0.5)

        if not cluster_available:
            raise AssertionError(
                f"[Step: {step_name}] Cluster did not become available within {timeout_seconds}s. "
                f"Unable to determine cluster state."
            )

        # Ждем стабильного состояния: один PRIMARY, один SYNCHRONIZED
        # Пробуем оба варианта (r1=PRIMARY или r2=PRIMARY)
        try:
            self.wait_for_cluster_state(
                self.bridge_client,
                {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED},
                timeout_seconds=timeout_seconds
            )
        except AssertionError:
            # Если r1 не PRIMARY, пробуем r2
            try:
                self.wait_for_cluster_state(
                    self.bridge_client,
                    {"r1": PileState.SYNCHRONIZED, "r2": PileState.PRIMARY},
                    timeout_seconds=timeout_seconds
                )
            except AssertionError as e:
                # Если оба варианта не сработали, получаем текущее состояние для диагностики
                try:
                    current_state = self.get_cluster_state(self.bridge_client)
                    current_states = {s.pile_name: s.state for s in current_state.pile_states}
                    raise AssertionError(
                        f"[Step: {step_name}] Failed to reach stable state (one PRIMARY, one SYNCHRONIZED). "
                        f"Current state: {current_states}. "
                        f"Original error: {e}"
                    ) from e
                except Exception as get_state_error:
                    raise AssertionError(
                        f"[Step: {step_name}] Failed to reach stable state and unable to get current state. "
                        f"Original error: {e}, Get state error: {get_state_error}"
                    ) from e

        # Получаем финальное состояние
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

    @staticmethod
    def check_states(result, expected_states):
        actual_states = {s.pile_name: s.state for s in result.pile_states}
        assert_that(actual_states, is_(has_entries(expected_states)))
        assert_that(len(actual_states), is_(len(expected_states)))
