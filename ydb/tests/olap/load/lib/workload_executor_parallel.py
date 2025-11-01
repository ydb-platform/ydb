import traceback
import allure
import logging
import os
import time as time_module
import yatest.common
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.remote_execution import (
    deploy_binaries_to_hosts,
)
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.utils import get_external_param
# Импортируем LoadSuiteBase чтобы наследоваться от него
from ydb.tests.olap.load.lib.conftest import LoadSuiteBase
from ydb.tests.olap.load.lib.workload_executor import WorkloadTestBase

from ydb.tests.stability.lib import deploy


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

    @classmethod
    def setup_class(cls) -> None:
        """
        Общая инициализация для workload тестов.
        """
        cls.base_executor = WorkloadTestBase()
        cls.base_executor.setup_class()

    def execute_parallel_workloads_test(
        self,
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
        preparation_result = self._prepare_stress_execution(workload_params, nodes_percentage)

        logging.debug(f"Deploy finished with {preparation_result}")

        # ФАЗА 2: ВЫПОЛНЕНИЕ
        execution_result = self._execute_stress_runs(
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

    def _prepare_stress_execution(
        self,
        workload_params: dict,
        nodes_percentage: int = 100,
    ):
        """
        ФАЗА 1: Подготовка к выполнению workload

        Args:
            workload_name: Имя workload для отчетов
            duration_value: Время выполнения в секундах
            use_chunks: Использовать ли разбивку на итерации (устаревший параметр)
            duration_param: Параметр для передачи времени выполнения
            nodes_percentage: Процент нод кластера для запуска workload

        Returns:
            Словарь с результатами подготовки
        """

        with allure.step("Phase 1: Prepare workload execution"):
            logging.info(
                f"Preparing execution: Nodes_percentage={nodes_percentage}%, mode=parallel"
            )

            # Останавливаем nemesis перед каждым запуском workload для чистого старта
            try:
                logging.info("Stopping nemesis service before workload execution for clean start")

                # Создаем сводный лог для Allure
                prep_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                nemesis_log = [f"Workload preparation started at {prep_time}"]

                # Останавливаем nemesis через общий метод
                self.__class__.base_executor._manage_nemesis(
                    False, "Stopping nemesis before workload execution", nemesis_log)

                logging.info("Nemesis stopped successfully before workload execution")

            except Exception as e:
                error_msg = f"Error stopping nemesis before workload execution: {e}"
                logging.error(error_msg)
                try:
                    allure.attach(
                        error_msg,
                        "Preparation - Nemesis Error",
                        attachment_type=allure.attachment_type.TEXT,
                    )
                except Exception:
                    pass

            # Сохраняем состояние нод для диагностики
            self.save_nodes_state()

            deployed_nodes = {}
            # Выполняем deploy на выбранный процент нод
            deploy_futures = []
            with ThreadPoolExecutor(max_workers=10) as tpe:
                for workload_name, workload_info in workload_params.items():
                    deploy_futures.append(
                        (
                            tpe.submit(self._deploy_workload_binary, workload_name, workload_info['local_path'], nodes_percentage),
                            workload_name
                        )
                    )

            total_hosts = []
            for deploy_future, future_workload_name in deploy_futures:
                deployed_nodes[future_workload_name] = deploy_future.result()
                total_hosts += list(map(lambda node: node['node'].host, deployed_nodes[future_workload_name]))

            # Инициализируем результат
            overall_result = YdbCliHelper.WorkloadRunResult()
            overall_result.start_time = time_module.time()

            logging.info(
                f"Preparation completed: {deployed_nodes} nodes in parallel mode"
            )

            return {
                "deployed_nodes": deployed_nodes,
                "total_hosts": set(total_hosts),
                "overall_result": overall_result,
                "workload_start_time": time_module.time(),
            }

    def _execute_stress_runs(
        self,
        workload_params: dict,
        duration_value: float,
        node_percentage: int,
        preparation_result: dict,
        nemesis: bool = False,
    ):
        """
        ФАЗА 2: Параллельное выполнение workload на всех нодах

        Args:
            workload_name: Имя workload для отчетов
            command_args_template: Шаблон аргументов командной строки
            duration_param: Параметр для передачи времени выполнения
            use_chunks: Использовать ли разбивку на итерации (устаревший параметр)
            preparation_result: Результаты подготовительной фазы
            nemesis: Запускать ли сервис nemesis

        Returns:
            Словарь с результатами выполнения
        """

        with allure.step("Phase 2: Execute workload runs in parallel"):
            deployed_nodes = preparation_result["deployed_nodes"]
            overall_result = preparation_result["overall_result"]

            if not deployed_nodes:
                logging.error("No deployed nodes available for execution")
                return {
                    "overall_result": overall_result,
                    "successful_runs": 0,
                    "total_runs": 0,
                    "total_execution_time": 0,
                    "workload_start_time": time_module.time(),
                    "deployed_nodes": [],
                }

            # Параллельное выполнение на всех нодах
            with allure.step(
                f"Execute workload in parallel on {preparation_result['total_hosts']} nodes"
            ):
                workload_start_time = time_module.time()

                # Запускаем nemesis через 15 секунд после начала выполнения
                # workload
                if nemesis:
                    # Устанавливаем флаг сразу при запуске nemesis потока
                    deploy._nemesis_started = True
                    logging.info("Nemesis flag set to True - will start in 15 seconds")

                    nemesis_thread = threading.Thread(
                        target=deploy._delayed_nemesis_start,
                        args=(15,),  # 15 секунд задержки
                        daemon=False,  # Убираем daemon=True чтобы поток не прерывался
                    )
                    nemesis_thread.start()
                    logging.info("Scheduled nemesis to start in 15 seconds")

                # Результаты выполнения для каждой ноды
                node_results = []

                # Функция для выполнения workload на одной ноде
                def execute_on_node(workload_config, stress_name, node):
                    node_host = node['node'].host
                    deployed_binary_path = node['binary_path']

                    node_result = {
                        "node": node['node'],
                        "stress_name": stress_name,
                        "host": node_host,
                        "successful_runs": 0,
                        "total_runs": 0,
                        "runs": [],
                        "total_execution_time": 0,
                    }

                    logging.info(f"Starting execution on node {node_host}")

                    start_time = time_module.time()
                    planned_end_time = start_time + duration_value

                    run_duration = duration_value
                    current_iteration = 0
                    # Выполняем план для этой ноды
                    while time_module.time() < planned_end_time:

                        # Используем формат iter_N без добавления префикса iter_ в _execute_single_workload_run
                        # так как он будет добавлен там
                        run_name = f"{stress_name}_{node_host}_iter_{current_iteration}"

                        # Устанавливаем флаг, что префикс iter_ уже добавлен
                        run_config_copy = {}
                        run_config_copy["iteration_num"] = current_iteration
                        run_config_copy["node_host"] = node_host
                        run_config_copy["duration"] = run_duration
                        run_config_copy["node_role"] = node['node'].role
                        run_config_copy["thread_id"] = (
                            node_host  # Идентификатор потока - хост ноды
                        )

                        # Выполняем один run
                        success, execution_time, stdout, stderr, is_timeout = (
                            self.__class__.base_executor._execute_single_workload_run(
                                deployed_binary_path,
                                node['node'],
                                run_name,
                                ' '.join(workload_config['args']),
                                '--duration',
                                run_config_copy,
                            )
                        )

                        # Сохраняем результат запуска
                        run_result = {
                            "run_num": current_iteration,
                            "run_config": run_config_copy,
                            "success": success,
                            "execution_time": execution_time,
                            "stdout": stdout,
                            "stderr": stderr,
                            "is_timeout": is_timeout,
                        }
                        node_result["runs"].append(run_result)
                        # Обновляем статистику ноды
                        node_result["total_execution_time"] += execution_time
                        if success:
                            node_result["successful_runs"] += 1
                            logging.info(
                                f"Run {current_iteration} on {node_host} completed successfully"
                            )
                        else:
                            logging.warning(
                                f"Run {current_iteration} on {node_host} failed")
                        current_iteration += 1
                        run_duration = planned_end_time - time_module.time()
                    node_result['total_runs'] = len(node_result["runs"])
                    logging.info(
                        f"Execution on {node_host} completed: "
                        f"{node_result['successful_runs']}/{node_result['total_runs']} successful"
                    )

                    return node_result

                # Запускаем параллельное выполнение на всех нодах
                with ThreadPoolExecutor(
                    max_workers=len(workload_params) * len(preparation_result["total_hosts"])
                ) as executor:
                    future_to_node = {}
                    for name, stress_config in workload_params.items():
                        for node in deployed_nodes[name]:
                            future_to_node[executor.submit(execute_on_node, stress_config, name, node)] = (name, node)

                    for future in as_completed(future_to_node):
                        try:
                            node_result = future.result()
                            node_results.append(node_result)
                        except Exception as e:
                            node_plan = future_to_node[future]
                            node_host = node_plan[1]['node'].host
                            logging.error(
                                f"Error executing on {node_host}: {e}")
                            logging.error(traceback.format_exc())
                            # Добавляем информацию об ошибке
                            node_results.append(
                                {
                                    "node": node_plan[1]['node'],
                                    "host": node_host,
                                    "stress_name": node_plan[0],
                                    "error": str(e),
                                    "successful_runs": 0,
                                    "total_runs": 1,
                                    "runs": [],
                                    "total_execution_time": 0,
                                }
                            )

                # Обрабатываем результаты всех нод
                successful_runs = 0
                total_runs = 0
                total_execution_time = 0

                # Обрабатываем результаты каждой ноды и добавляем их в общий
                # результат
                for node_idx, node_result in enumerate(node_results):
                    node_host = node_result["host"]

                    # Добавляем статистику ноды в общую статистику
                    successful_runs += node_result["successful_runs"]
                    total_runs += node_result["total_runs"]
                    total_execution_time += node_result["total_execution_time"]

                    # Обрабатываем результаты каждого запуска на этой ноде
                    for run_result in node_result.get("runs", []):
                        # Генерируем уникальный номер запуска для всех нод
                        global_run_num = (
                            node_idx * 10000 +
                            run_result["run_num"]
                        )

                        # Обрабатываем результат запуска
                        self.__class__.base_executor._process_single_run_result(
                            overall_result,
                            node_result["stress_name"],
                            global_run_num,
                            run_result["run_config"],
                            run_result["success"],
                            run_result["execution_time"],
                            run_result["stdout"],
                            run_result["stderr"],
                            run_result["is_timeout"],
                        )

                logging.info(
                    f"Parallel execution completed: {successful_runs}/{total_runs} successful runs "
                    f"on {len(node_results)} nodes"
                )

                return {
                    "overall_result": overall_result,
                    "successful_runs": successful_runs,
                    "total_runs": total_runs,
                    "total_execution_time": total_execution_time,
                    "workload_start_time": workload_start_time,
                    "deployed_nodes": deployed_nodes,
                    "node_results": node_results,
                }

    def _deploy_workload_binary(self, workload_name: str, workload_path: str, nodes_percentage: int = 100):
        """
        Выполняет deploy workload binary на указанный процент нод кластера

        Args:
            workload_name: Имя workload для отчетов
            nodes_percentage: Процент нод кластера для деплоя (от 1 до 100)

        Returns:
            Список словарей с информацией о нодах, на которые выполнен деплой:
            [{'node': node_object, 'binary_path': path_to_binary}, ...]
        """
        with allure.step("Deploy workload binary"):
            logging.info(
                f"Starting deployment for {workload_name} on {nodes_percentage}% of nodes"
            )

            # Получаем бинарный файл
            with allure.step("Get workload binary"):
                logging.info(f"Binary path from: {workload_path}")
                binary_files = [
                    yatest.common.binary_path(workload_path)
                ]
                allure.attach(
                    f"Binary path: {binary_files[0]}",
                    "Binary Path",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info(f"Binary path resolved: {binary_files[0]}")

            # Получаем уникальные хосты кластера
            with allure.step("Select unique cluster hosts"):
                nodes = YdbCluster.get_cluster_nodes()
                if not nodes:
                    raise Exception("No cluster nodes found")

                # Собираем уникальные хосты и соответствующие им ноды
                unique_hosts = {}
                for node in nodes:
                    if node.host not in unique_hosts:
                        unique_hosts[node.host] = node

                unique_nodes = list(unique_hosts.values())

                # Определяем количество нод для деплоя на основе процента
                num_nodes = max(
                    1, int(
                        len(unique_nodes) * nodes_percentage / 100))
                # Выбираем первые N нод из списка уникальных
                selected_nodes = unique_nodes[:num_nodes]

                allure.attach(
                    f"Selected {
                        len(selected_nodes)} / {
                        len(unique_nodes)} unique hosts ({nodes_percentage} % )",
                    "Target Hosts",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info(
                    f"Selected {
                        len(selected_nodes)} / {
                        len(unique_nodes)} unique hosts for deployment"
                )

            # Развертываем бинарный файл на выбранных нодах
            with allure.step(
                f"Deploy {
                    workload_name} to {
                    len(selected_nodes)} hosts"
            ):
                target_hosts = [node.host for node in selected_nodes]
                logging.info(f"Starting deployment to hosts: {target_hosts}")

                deploy_results = deploy_binaries_to_hosts(
                    binary_files, target_hosts, self.binaries_deploy_path
                )
                logging.info(f"Deploy results: {deploy_results}")

                # Собираем информацию о результатах деплоя
                deployed_nodes = []
                failed_nodes = []

                for node in selected_nodes:
                    binary_result = deploy_results.get(node.host, {}).get(os.path.basename(binary_files[0]), {})
                    success = binary_result.get("success", False)

                    if success:
                        deployed_nodes.append(
                            {"node": node, "binary_path": binary_result["path"]}
                        )
                        logging.info(
                            f"Deployment successful on {
                                node.host}: {
                                binary_result['path']}"
                        )
                    else:
                        failed_nodes.append(
                            {
                                "node": node,
                                "error": binary_result.get("error", "Unknown error"),
                            }
                        )
                        logging.error(
                            f"Deployment failed on {
                                node.host}: {
                                binary_result.get(
                                    'error',
                                    'Unknown error')}"
                        )

                # Прикрепляем детали развертывания
                allure.attach(
                    f"Successful deployments: {
                        len(deployed_nodes)} / {
                        len(selected_nodes)}\n"
                    + f"Failed deployments: {len(failed_nodes)}/{len(selected_nodes)}",
                    "Deployment Summary",
                    attachment_type=allure.attachment_type.TEXT,
                )

                # Проверяем, что хотя бы одна нода успешно развернута
                if not deployed_nodes:
                    # Создаем детальное сообщение об ошибке деплоя
                    deploy_error_details = []
                    deploy_error_details.append(
                        f"DEPLOYMENT FAILED: {workload_name}"
                    )
                    deploy_error_details.append(
                        f"Target hosts: {target_hosts}")
                    deploy_error_details.append(
                        f"Target directory: {self.binaries_deploy_path}"
                    )
                    deploy_error_details.append(
                        f"Local binary path: {binary_files[0]}")

                    # Детали ошибок
                    deploy_error_details.append("\nDeployment errors:")
                    for failed in failed_nodes:
                        deploy_error_details.append(
                            f"  {failed['node'].host}: {failed['error']}"
                        )

                    detailed_deploy_error = "\n".join(deploy_error_details)
                    logging.error(detailed_deploy_error)
                    raise Exception(detailed_deploy_error)

                logging.info(
                    f"Binary deployed successfully to {
                        len(deployed_nodes)} unique hosts"
                )
                return deployed_nodes

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
            self.__class__.base_executor._analyze_execution_results(
                overall_result, successful_runs, total_runs, False
            )

            # Собираем и добавляем статистику
            self.__class__.base_executor._add_execution_statistics(
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
