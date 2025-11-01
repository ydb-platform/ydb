import allure
import logging
import time as time_module
import pytest

from ydb.tests.library.stability.aggregate_results import add_execution_statistics, analyze_execution_results
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.utils import external_param_is_true, get_external_param
# Импортируем LoadSuiteBase чтобы наследоваться от него
from ydb.tests.olap.load.lib.conftest import LoadSuiteBase
from ydb.tests.library.stability.deploy import StressUtilDeployer
from ydb.tests.library.stability.run_stress import StressRunExecutor


class ParallelWorkloadTestBase(LoadSuiteBase):
    """
    Базовый класс для workload тестов с общей функциональностью
    """

    # Переопределяемые атрибуты в наследниках
    binaries_deploy_path: str = (
        "/tmp/stress_binaries/"  # Путь для деплоя бинарных файлов
    )
    timeout: float = 1800.0  # Таймаут по умолчанию
    _nemesis_started: bool = False  # Флаг для отслеживания запуска nemesis
    cluster_path: str = str(
        get_external_param(
            "cluster_path",
            ""))  # Путь к кластеру
    yaml_config: str = str(
        get_external_param(
            "yaml-config",
            ""))  # Путь к yaml конфигурации

    @pytest.fixture(autouse=True, scope="session")
    def binary_deployer(self) -> StressUtilDeployer:
        binaries_deploy_path: str = (
            "/tmp/stress_binaries/"
        )
        return StressUtilDeployer(binaries_deploy_path)

    @pytest.fixture(autouse=True, scope="session")
    def stress_executor(self) -> StressRunExecutor:
        return StressRunExecutor()

    @pytest.fixture(autouse=True, scope="session")
    def olap_load_base(self) -> LoadSuiteBase:
        base = LoadSuiteBase()
        try:
            pass
            # LoadSuiteBase.do_setup_class()
        except Exception as e:
            logging.error(f"Error in do_setup_class: {e}")
            raise
        return base

    @pytest.fixture(autouse=True, scope="session")
    def context_setup(self, binary_deployer, olap_load_base) -> None:
        """
        Общая инициализация для workload тестов.
        НЕ выполняем _Verification здесь - будем делать это перед каждым тестом.
        """
        with allure.step("Workload test setup: initialize"):
            self._setup_start_time = time_module.time()
            self._ignore_stderr_content = external_param_is_true('ignore_stderr_content')

    def execute_parallel_workloads_test(
        self,
        stress_executor: StressRunExecutor,
        stress_deployer: StressUtilDeployer,
        workload_params: dict,
        duration_value: float = None,
        nemesis_enabled: bool = False,
        nodes_percentage: int = 100,
    ):
        """
        Выполняет полный цикл workload теста

        Args:
            workload_params: Имя workload для отчетов
            duration_value: Время выполнения в секундах (если None, используется self.timeout)
            nemesis_enabled: Запускать ли сервис nemesis через 15 секунд после начала выполнения workload
            nodes_percentage: Процент нод кластера для запуска workload (от 1 до 100)
        """

        if duration_value is None:
            duration_value = self.timeout

        # Проверяем корректность процента нод
        if nodes_percentage < 1 or nodes_percentage > 100:
            raise ValueError(
                f"nodes_percentage должен быть от 1 до 100, получено: {nodes_percentage}"
            )

        additional_stats = {}
        additional_stats["nemesis_enabled"] = nemesis_enabled
        additional_stats["nodes_percentage"] = nodes_percentage

        logging.info("=== Starting env preparation ===")

        # ФАЗА 1: ПОДГОТОВКА
        preparation_result = stress_deployer.prepare_stress_execution(self, workload_params, nodes_percentage)

        logging.debug(f"Deploy finished with {preparation_result}")

        # ФАЗА 2: ВЫПОЛНЕНИЕ
        execution_result = stress_executor.execute_stress_runs(
            stress_deployer,
            workload_params,
            duration_value,
            nodes_percentage,
            preparation_result,
            nemesis_enabled,
        )
        logging.debug(f"Execution finished with {execution_result}")
        logging.debug(f"Additional stats {additional_stats}")

        # ФАЗА 3: РЕЗУЛЬТАТЫ
        final_result = self._finalize_workload_results(
            workload_params,
            execution_result,
            additional_stats,
            duration_value
        )

        logging.info("=== Workload test completed ===")
        logging.debug(f"Execution final result {final_result}")
        # return final_result

    def _finalize_workload_results(
        self,
        workload_params: dict,
        execution_result: dict,
        additional_stats: dict,
        duration_value: float
    ):
        """
        ФАЗА 3: Финализация результатов и диагностика

        Args:
            workload_name: Имя workload для отчетов
            execution_result: Результаты выполнения
            additional_stats: Дополнительная статистика
            duration_value: Время выполнения в секундах
            use_chunks: Использовать ли разбивку на итерации (устаревший параметр)

        Returns:
            Финальный результат выполнения
        """

        with allure.step("Phase 3: Finalize results and diagnostics"):
            overall_result = execution_result["overall_result"]
            successful_runs = execution_result["successful_runs"]
            total_runs = execution_result["total_runs"]

            # Анализируем результаты и добавляем ошибки/предупреждения
            analyze_execution_results(
                overall_result, successful_runs, total_runs, False
            )

            # Собираем и добавляем статистику
            add_execution_statistics(
                overall_result,
                'parallel_run',
                execution_result,
                additional_stats,
                duration_value,
                False
            )

            # Финальная обработка с диагностикой (подготавливает данные для выгрузки)
            overall_result.workload_start_time = execution_result["workload_start_time"]
            self.process_workload_result_with_diagnostics(
                overall_result, 'parallel_run', execution_result
            )

            # Финальная обработка статуса (может выбросить исключение, но результаты уже выгружены)
            # Используем node_errors, сохраненные из диагностики
            node_errors = getattr(overall_result, '_node_errors', [])
            self._handle_final_status(overall_result, 'parallel_run', node_errors)

            # Отдельный шаг для выгрузки результатов (ПОСЛЕ подготовки всех данных)
            # self.__class__._safe_upload_results(overall_result, 'all')

            logging.info(
                f"Final result: success={
                    overall_result.success}, successful_runs={successful_runs} / {total_runs}"
            )
            return overall_result

    def process_workload_result_with_diagnostics(
        self,
        result: YdbCliHelper.WorkloadRunResult,
        workload_name: str,
        execution_result=None
    ):
        """
        Обрабатывает результат workload с добавлением диагностической информации

        Args:
            result: Результат выполнения workload
            workload_name: Имя workload для отчетов
            check_scheme: Проверять ли схему базы данных
            use_node_subcols: Использовать ли подколонки для нод в таблице итераций
        """
        with allure.step(f"Process workload result for {workload_name}"):
            # Собираем информацию о параметрах workload для заголовка
            workload_params = {}

            # Извлекаем параметры из статистики
            workload_stats = result.get_stats(workload_name)
            if workload_stats:
                # Добавляем важные параметры в начало
                important_params = [
                    "total_runs",
                    "planned_duration",
                    "actual_duration",
                    "use_iterations",
                    "workload_type",
                    "table_type",
                ]
                for param in important_params:
                    if param in workload_stats:
                        workload_params[param] = workload_stats[param]

                # Информация об итерациях и потоках
                if "total_iterations" in workload_stats:
                    workload_params["total_iterations"] = workload_stats[
                        "total_iterations"
                    ]
                if "total_threads" in workload_stats:
                    workload_params["total_threads"] = workload_stats["total_threads"]

                # Добавляем остальные параметры
                for key, value in workload_stats.items():
                    if key not in workload_params and key not in [
                        "success_rate",
                        "successful_runs",
                        "failed_runs",
                    ]:
                        workload_params[key] = value

            # Собираем информацию об ошибках нод
            node_errors = []
            verify_errors = {}

            # Проверяем состояние нод и собираем ошибки
            try:
                # Используем метод родительского класса для диагностики нод
                end_time = time_module.time()
                diagnostics_start_time = getattr(
                    result, "workload_start_time", result.start_time
                )
                verify_errors = self.check_nodes_verifies_with_timing(diagnostics_start_time, end_time)
                node_errors = self.check_nodes_diagnostics_with_timing(
                    result, diagnostics_start_time, end_time
                )

            except Exception as e:
                logging.error(f"Error getting nodes state: {e}")
                # Добавляем ошибку в результат
                result.add_warning(f"Error getting nodes state: {e}")
                node_errors = []  # Устанавливаем пустой список если диагностика не удалась

            # Вычисляем время выполнения
            end_time = time_module.time()

            # Добавляем дополнительную информацию для отчета
            additional_table_strings = {}

            # Добавляем информацию о фактическом времени выполнения
            if workload_params.get("actual_duration") is not None:
                actual_duration = workload_params["actual_duration"]
                planned_duration = workload_params.get(
                    "planned_duration", self.timeout)

                # Форматируем время в минуты и секунды
                actual_minutes = int(actual_duration) // 60
                actual_seconds = int(actual_duration) % 60
                planned_minutes = int(planned_duration) // 60
                planned_seconds = int(planned_duration) % 60

                # Добавляем информацию о времени выполнения
                additional_table_strings["execution_time"] = (
                    f"Actual: {actual_minutes}m {actual_seconds}s (Planned: {planned_minutes}m {planned_seconds}s)"
                )

            # Информация об итерациях и потоках
            if (
                "total_iterations" in workload_params
                and "total_threads" in workload_params
            ):
                total_iterations = workload_params["total_iterations"]
                total_threads = workload_params["total_threads"]

                if total_iterations == 1 and total_threads > 1:
                    additional_table_strings["execution_mode"] = (
                        f"Single iteration with {total_threads} parallel threads"
                    )
                elif total_iterations > 1:
                    avg_threads = workload_params.get(
                        "avg_threads_per_iteration", 1)
                    additional_table_strings["execution_mode"] = (
                        f"{total_iterations} iterations with avg {
                            avg_threads: .1f} threads per iteration"
                    )

            # --- ВАЖНО: выставляем nodes_with_issues для корректного fail ---
            stats = result.get_stats(workload_name)
            if stats is not None:
                result.add_stat(
                    workload_name,
                    "nodes_with_issues",
                    len(node_errors))

                # Формируем списки ошибок для выгрузки
                node_error_messages = []
                workload_error_messages = []

                # Собираем ошибки нод с подробностями
                for node_error in node_errors:
                    if node_error.core_hashes:
                        for core_id, core_hash in node_error.core_hashes:
                            node_error_messages.append(f"Node {node_error.node.slot} coredump {core_id}")
                    if node_error.was_oom:
                        node_error_messages.append(f"Node {node_error.node.slot} experienced OOM")
                    if hasattr(node_error, 'verifies') and node_error.verifies > 0:
                        node_error_messages.append(f"Node {node_error.node.host} had {node_error.verifies} VERIFY fails")
                    if hasattr(node_error, 'sanitizer_errors') and node_error.sanitizer_errors > 0:
                        node_error_messages.append(f"Node {node_error.node.host} has {node_error.sanitizer_errors} SAN errors")

                # Собираем workload ошибки (не связанные с нодами)
                if result.errors:
                    for err in result.errors:
                        if "coredump" not in err.lower() and "oom" not in err.lower():
                            workload_error_messages.append(err)

                # Добавляем в статистику
                result.add_stat(workload_name, "node_error_messages", node_error_messages)
                result.add_stat(workload_name, "workload_error_messages", workload_error_messages)

                # Добавляем boolean флаги
                result.add_stat(workload_name, "node_errors", len(node_error_messages) > 0)
                result.add_stat(workload_name, "workload_errors", len(workload_error_messages) > 0)

                # Собираем workload предупреждения (исключая node-специфичные)
                workload_warning_messages = []
                if result.warnings:
                    for warn in result.warnings:
                        if "coredump" not in warn.lower() and "oom" not in warn.lower():
                            workload_warning_messages.append(warn)

                result.add_stat(workload_name, "workload_warning_messages", workload_warning_messages)
                result.add_stat(workload_name, "workload_warnings", len(workload_warning_messages) > 0)

            # 3. Формирование summary/статистики (with_errors/with_warnings автоматически добавляются в ydb_cli.py)

            # 4. Формирование allure-отчёта
            self._create_parallel_allure_report(result, workload_name, workload_params, node_errors, verify_errors, execution_result)

            # Сохраняем node_errors для использования после выгрузки
            result._node_errors = node_errors

            # Данные подготовлены, теперь можно выгружать результаты
