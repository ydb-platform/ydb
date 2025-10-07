import traceback
import allure
import logging
import os
import time as time_module
import uuid
import yatest.common
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.remote_execution import (
    execute_command,
    deploy_binaries_to_hosts,
)
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.utils import get_external_param
# Импортируем LoadSuiteBase чтобы наследоваться от него
from ydb.tests.olap.load.lib.conftest import LoadSuiteBase


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
        with allure.step("Workload test setup: initialize"):
            cls._setup_start_time = time_module.time()

            # Наследуемся от LoadSuiteBase
            super().setup_class()

    @classmethod
    def do_teardown_class(cls):
        """
        Общая очистка для workload тестов.
        Останавливает процессы workload на всех нодах кластера.
        Останавливает nemesis, если он был запущен.
        """
        with allure.step("Workload test teardown: cleanup processes and nemesis"):
            if not cls.workload_binary_names:
                logging.warning(
                    f"workload_binary_names not set for {
                        cls.__name__}, skipping process cleanup"
                )
                allure.attach(
                    f"workload_binary_names not set for {
                        cls.__name__}, skipping process cleanup",
                    "Teardown - Skip Cleanup",
                    attachment_type=allure.attachment_type.TEXT,
                )
                return

            logging.info(
                f"Starting {
                    cls.__name__} teardown: stopping workload processes"
            )

            try:
                # Используем метод из LoadSuiteBase напрямую через наследование
                for workload_binary_name in cls.workload_binary_names:
                    cls.kill_workload_processes(
                        process_names=[
                            cls.binaries_deploy_path +
                            workload_binary_name],
                        target_dir=cls.binaries_deploy_path,
                    )

                # Останавливаем nemesis, если он был запущен
                if getattr(cls, "_nemesis_started", False):
                    logging.info("Stopping nemesis service (flag was set)")
                    cls._stop_nemesis()
                else:
                    # Дополнительная проверка - если nemesis был запущен в тесте,
                    # но флаг не установлен (например, поток был прерван)
                    logging.info("Nemesis flag not set, but checking if nemesis was started in test")
                    # Останавливаем nemesis в любом случае для безопасности
                    cls._stop_nemesis()

            except Exception as e:
                error_msg = f"Error during teardown: {e}"
                logging.error(error_msg)
                allure.attach(
                    error_msg,
                    "Teardown Error",
                    attachment_type=allure.attachment_type.TEXT,
                )

    @classmethod
    def _stop_nemesis(cls):
        """Останавливает сервис nemesis на всех нодах кластера"""
        with allure.step("Stop nemesis service on all cluster nodes"):
            try:
                # Используем общий метод управления nemesis
                nemesis_log = []
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                nemesis_log.append(
                    f"Stopping nemesis service at {current_time}")

                cls._manage_nemesis(
                    False, "Stopping nemesis during teardown", nemesis_log
                )
                cls._nemesis_started = False
                YdbCluster.wait_ydb_alive(120)

            except Exception as e:
                error_msg = f"Error stopping nemesis: {e}"
                logging.error(error_msg)
                try:
                    allure.attach(
                        error_msg,
                        "Nemesis Stop Error",
                        attachment_type=allure.attachment_type.TEXT,
                    )
                except Exception:
                    pass


    def create_workload_result(
        self,
        workload_name: str,
        stdout: str,
        stderr: str,
        success: bool,
        additional_stats: dict = None,
        is_timeout: bool = False,
        iteration_number: int = 0,
        actual_execution_time: float = None,
    ) -> YdbCliHelper.WorkloadRunResult:
        """
        Создает и заполняет WorkloadRunResult с общей логикой

        Args:
            workload_name: Имя workload для статистики
            stdout: Вывод workload
            stderr: Ошибки workload
            success: Успешность выполнения
            additional_stats: Дополнительная статистика
            is_timeout: Флаг таймаута
            iteration_number: Номер итерации
            actual_execution_time: Фактическое время выполнения

        Returns:
            Заполненный WorkloadRunResult
        """
        result = YdbCliHelper.WorkloadRunResult()
        result.start_time = self.__class__._setup_start_time
        result.stdout = str(stdout)
        result.stderr = str(stderr)

        # Добавляем диагностическую информацию о входных данных
        logging.info(f"Creating workload result for {workload_name}")
        logging.info(
            f"Input parameters - success: {success}, stdout length: {
                len(
                    str(stdout))}, stderr length: {
                len(
                    str(stderr))}"
        )
        if stdout:
            logging.info(
                f"Stdout preview(first 200 chars): {
                    str(stdout)[
                        :200]}")
        if stderr:
            logging.info(
                f"Stderr preview(first 200 chars): {
                    str(stderr)[
                        :200]}")

        # Анализируем результаты выполнения
        error_found = False

        # Проверяем на timeout сначала (это warning, не error)
        if is_timeout:
            result.add_warning(
                f"Workload execution timed out. stdout: {stdout}, stderr: {stderr}"
            )
        else:
            # Проверяем явные ошибки (только если не timeout)
            if not success:
                result.add_error(
                    f"Workload execution failed. stderr: {stderr}")
                error_found = True
            elif "error" in str(stderr).lower():
                result.add_error(f"Error detected in stderr: {stderr}")
                error_found = True
            elif self._has_real_error_in_stdout(str(stdout)):
                result.add_warning(f"Error detected in stdout: {stdout}")
                error_found = True

        # Проверяем предупреждения
        if (
            "warning: permanently added" not in str(stderr).lower()
            and "warning" in str(stderr).lower()
        ):
            result.add_warning(f"Warning in stderr: {stderr}")

        # Добавляем информацию о выполнении в iterations
        iteration = YdbCliHelper.Iteration()
        # Используем фактическое время выполнения, если оно указано, иначе
        # плановое время
        execution_time = (
            actual_execution_time if actual_execution_time is not None else self.timeout
        )
        iteration.time = execution_time

        # Добавляем имя итерации для лучшей идентификации
        if additional_stats:
            node_host = additional_stats.get("node_host", "")
            iteration_num = additional_stats.get(
                "iteration_num", iteration_number)

            # Проверяем, был ли уже добавлен префикс iter_ в имя
            iter_prefix_added = additional_stats.get(
                "iter_prefix_added", False)

            # Формируем имя итерации
            if iter_prefix_added:
                # Если префикс уже добавлен, просто используем имя workload
                iteration.name = f"{workload_name}_{node_host}"
            else:
                # Если префикс еще не добавлен, добавляем его
                iteration.name = f"{workload_name}_{node_host}_iter_{iteration_num}"

            # Добавляем статистику об итерации и потоке в итерацию
            if not hasattr(iteration, "stats"):
                iteration.stats = {}

            iteration_stats = {
                "iteration_info": {
                    "iteration_num": iteration_num,
                    "node_host": node_host,
                    "thread_id": node_host,  # Идентификатор потока - хост ноды
                    # Добавляем фактическое время выполнения
                    "actual_execution_time": execution_time,
                }
            }

            # Добавляем статистику в итерацию
            iteration.stats.update(iteration_stats)

        if is_timeout:
            # Для timeout используем более конкретное сообщение
            iteration.error_message = "Workload execution timed out"
        elif error_found:
            # Устанавливаем ошибку в iteration для consistency
            iteration.error_message = result.error_message

        result.iterations[iteration_number] = iteration

        # Добавляем базовую статистику
        result.add_stat(
            workload_name, "execution_time", execution_time
        )  # Используем фактическое время
        result.add_stat(
            workload_name, "planned_duration", self.timeout
        )  # Добавляем плановую длительность отдельно
        # Timeout не считается неуспешным выполнением, это warning
        result.add_stat(
            workload_name,
            "workload_success",
            (success and not error_found) or is_timeout,
        )
        result.add_stat(
            workload_name,
            "success_flag",
            success)  # Исходный флаг успеха

        # Добавляем дополнительную статистику если есть
        if additional_stats:
            for key, value in additional_stats.items():
                result.add_stat(workload_name, key, value)

        logging.info(
            f"Workload result created - final success: {
                result.success}, error_message: {
                result.error_message}"
        )

        return result

    def _has_real_error_in_stdout(self, stdout: str) -> bool:
        """
        Проверяет, есть ли в stdout настоящие ошибки, исключая статистику workload

        Args:
            stdout: Вывод stdout для анализа

        Returns:
            bool: True если найдены настоящие ошибки
        """
        if not stdout:
            return False

        stdout_lower = stdout.lower()

        # Список паттернов, которые НЕ являются ошибками (статистика workload)
        false_positive_patterns = [
            "error responses count:",  # статистика ответов
            "_error responses count:",  # например scheme_error responses count
            "error_responses_count:",  # альтернативный формат
            "errorresponsescount:",  # без разделителей
            "errors encountered:",  # может быть частью статистики
            "total errors:",  # общая статистика
            "error rate:",  # метрика частоты ошибок
            "error percentage:",  # процент ошибок
            "error count:",  # счетчик ошибок в статистике
            "scheme_error responses count:",  # конкретный пример из статистики
            "aborted responses count:",  # другие типы ответов из статистики
            "precondition_failed responses count:",  # еще один тип из статистики
            "error responses:",  # краткий формат
            "eventkind:",  # статистика по событиям
            "success responses count:",  # для контекста успешных ответов
            "responses count:",  # общий паттерн счетчиков ответов
        ]

        # Проверяем, есть ли слово "error" в контексте, который НЕ является
        # ложным срабатыванием
        error_positions = []
        start_pos = 0
        while True:
            pos = stdout_lower.find("error", start_pos)
            if pos == -1:
                break
            error_positions.append(pos)
            start_pos = pos + 1

        # Для каждого найденного "error" проверяем контекст
        for pos in error_positions:
            # Берем контекст вокруг найденного "error" (50 символов до и после)
            context_start = max(0, pos - 50)
            context_end = min(len(stdout), pos + 50)
            context = stdout[context_start:context_end].lower()

            # Проверяем, является ли это ложным срабатыванием
            is_false_positive = any(
                pattern in context for pattern in false_positive_patterns
            )

            if not is_false_positive:
                # Дополнительные проверки на реальные ошибочные сообщения
                real_error_indicators = [
                    "fatal:",  # "Fatal: something went wrong"
                    "error:",  # "Error: something went wrong"
                    "error occurred",  # "An error occurred"
                    "error while",  # "Error while processing"
                    "error during",  # "Error during execution"
                    "fatal error",  # "Fatal error"
                    "runtime error",  # "Runtime error"
                    "execution error",  # "Execution error"
                    "internal error",  # "Internal error"
                    "connection error",  # "Connection error"
                    "timeout error",  # "Timeout error"
                    "failed with error",  # "Operation failed with error"
                    "exceptions must derive from baseexception",  # Python exception error
                ]

                # Если найден реальный индикатор ошибки, возвращаем True
                if any(indicator in context for indicator in real_error_indicators):
                    return True

        return False

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

        logging.info(
            f"=== Starting env preparation ===")

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

        logging.info(f"=== Workload test completed ===")
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
                self.__class__._manage_nemesis(
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
                total_hosts += map(lambda node: node['node'].host, deployed_nodes[future_workload_name])

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
        duration_value: str,
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
                    self.__class__._nemesis_started = True
                    logging.info("Nemesis flag set to True - will start in 15 seconds")

                    nemesis_thread = threading.Thread(
                        target=self._delayed_nemesis_start,
                        args=(15,),  # 15 секунд задержки
                        daemon=False,  # Убираем daemon=True чтобы поток не прерывался
                    )
                    nemesis_thread.start()
                    logging.info("Scheduled nemesis to start in 15 seconds")

                # Результаты выполнения для каждой ноды
                node_results = []

                # Блокировка для безопасного обновления общих данных из потоков
                threading.Lock()

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
                    current_iteraton = 0
                    # Выполняем план для этой ноды
                    while time_module.time() < planned_end_time:

                        # Используем формат iter_N без добавления префикса iter_ в _execute_single_workload_run
                        # так как он будет добавлен там
                        run_name = f"{stress_name}_{node_host}_iter_{current_iteraton}"

                        # Устанавливаем флаг, что префикс iter_ уже добавлен
                        run_config_copy = {}
                        run_config_copy["iteration_num"] = current_iteraton
                        run_config_copy["node_host"] = node_host
                        run_config_copy["duration"] = run_duration
                        run_config_copy["node_role"] = node['node'].role
                        run_config_copy["thread_id"] = (
                            node_host  # Идентификатор потока - хост ноды
                        )

                        # Выполняем один run
                        success, execution_time, stdout, stderr, is_timeout = (
                            self._execute_single_workload_run(
                                deployed_binary_path,
                                node_host,
                                run_name,
                                ' '.join(workload_config['args']),
                                run_config_copy,
                            )
                        )

                        # Сохраняем результат запуска
                        run_result = {
                            "run_num": current_iteraton,
                            "run_config": run_config_copy,
                            "success": success,
                            "execution_time": execution_time,
                            "stdout": stdout,
                            "stderr": '',#stderr,
                            "is_timeout": is_timeout,
                        }
                        node_result["runs"].append(run_result)
                        # Обновляем статистику ноды
                        node_result["total_execution_time"] += execution_time
                        if success:
                            node_result["successful_runs"] += 1
                            logging.info(
                                f"Run {current_iteraton} on {node_host} completed successfully"
                            )
                        else:
                            logging.warning(
                                f"Run {current_iteraton} on {node_host} failed")
                        current_iteraton += 1
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
                        self._process_single_run_result(
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

    def _execute_single_workload_run(
        self,
        deployed_binary_path: str,
        target_node,
        run_name: str,
        command_args_template: str,
        run_config: dict,
    ):
        """
        Выполняет один запуск workload

        Args:
            deployed_binary_path: Путь к бинарному файлу workload
            target_node: Нода для выполнения
            run_name: Базовое имя запуска
            command_args_template: Шаблон аргументов командной строки
            duration_param: Параметр для передачи времени выполнения
            run_config: Конфигурация запуска с информацией об итерации

        Returns:
            Кортеж (успех, время выполнения, stdout, stderr, флаг таймаута)
        """
        # Подставляем переменные в command_args_template для уникальности путей
        command_args = self._substitute_variables_in_template(
            command_args_template, target_node, run_config
        )
    
        command_args = (
            f"{command_args} --duration {run_config['duration']}"
        )

        # Добавляем информацию об итерации в имя запуска только если префикс
        # еще не добавлен

        run_start_time = time_module.time()

        try:
            with allure.step(f"Execute {run_name}"):
                allure.attach(
                    f"Run config: {run_config}",
                    "Run Info",
                    attachment_type=allure.attachment_type.TEXT,
                )
                allure.attach(
                    command_args,
                    "Command Arguments",
                    attachment_type=allure.attachment_type.TEXT,
                )
                allure.attach(
                    f"Target host: {target_node}",
                    "Execution Target",
                    attachment_type=allure.attachment_type.TEXT,
                )

                # Формируем и выполняем команду
                with allure.step("Execute workload command"):
                    # Отключаем буферизацию для гарантии захвата вывода
                    cmd = f"stdbuf -o0 -e0 {deployed_binary_path} {command_args}"

                    run_timeout = (
                        run_config["duration"] + 150
                    )  # Добавляем буфер для завершения

                    allure.attach(
                        cmd, "Full Command", attachment_type=allure.attachment_type.TEXT
                    )
                    allure.attach(
                        f"Timeout: {int(run_timeout)}s",
                        "Execution Timeout",
                        attachment_type=allure.attachment_type.TEXT,
                    )

                    execution_result = execute_command(
                        target_node,
                        cmd,
                        raise_on_error=False,
                        timeout=int(run_timeout),
                        raise_on_timeout=False,
                    )

                    stdout = execution_result.stdout
                    stderr = execution_result.stderr
                    is_timeout = execution_result.is_timeout

                    # Прикрепляем результаты выполнения команды
                    if stdout:
                        allure.attach(
                            stdout,
                            "Command Stdout",
                            attachment_type=allure.attachment_type.TEXT,
                        )
                    else:
                        allure.attach(
                            "(empty)",
                            "Command Stdout",
                            attachment_type=allure.attachment_type.TEXT,
                        )

                    if stderr:
                        allure.attach(
                            stderr,
                            "Command Stderr",
                            attachment_type=allure.attachment_type.TEXT,
                        )
                    else:
                        allure.attach(
                            "(empty)",
                            "Command Stderr",
                            attachment_type=allure.attachment_type.TEXT,
                        )

                    # success=True только если stderr пустой (исключая SSH
                    # warnings) И нет timeout
                    success = not bool(stderr.strip()) and not is_timeout

                    execution_time = time_module.time() - run_start_time

                    logging.info(
                        f"{run_name} completed in {
                            execution_time: .1f}s, success: {success}, timeout: {is_timeout}"
                    )

                    return success, execution_time, stdout, stderr, is_timeout

        except Exception as e:
            execution_time = time_module.time() - run_start_time
            error_msg = f"Exception in {run_name}: {e}"
            logging.error(error_msg)
            return False, execution_time, "", error_msg, False

    def _substitute_variables_in_template(
        self,
        command_args_template: str,
        target_node,
        run_config: dict
    ) -> str:
        """
        Подставляет переменные в шаблон command_args

        Поддерживаемые переменные:
        - {node_host} - хост ноды
        - {iteration_num} - номер итерации
        - {thread_id} - ID потока (обычно хост ноды)
        - {run_id} - уникальный ID запуска
        - {timestamp} - timestamp запуска
        - {uuid} - короткий UUID

        Args:
            command_args_template: Шаблон аргументов командной строки
            target_node: Нода для выполнения
            run_config: Конфигурация запуска

        Returns:
            Строка с подставленными переменными
        """

        # Получаем значения переменных
        node_host = target_node
        iteration_num = run_config.get("iteration_num", 1)
        thread_id = run_config.get("thread_id", node_host)
        timestamp = int(time_module.time())
        short_uuid = uuid.uuid4().hex[:8]

        # Создаем уникальный run_id
        run_id = f"{node_host}_{iteration_num}_{timestamp}"

        # Словарь подстановок
        substitutions = {
            "{node_host}": node_host,
            "{iteration_num}": str(iteration_num),
            "{thread_id}": str(thread_id),
            "{run_id}": run_id,
            "{timestamp}": str(timestamp),
            "{uuid}": short_uuid,
        }

        # Выполняем подстановки
        result = command_args_template
        for placeholder, value in substitutions.items():
            result = result.replace(placeholder, value)

        return result

    def _process_single_run_result(
        self,
        overall_result,
        workload_name: str,
        run_num: int,
        run_config: dict,
        success: bool,
        execution_time: float,
        stdout: str,
        stderr: str,
        is_timeout: bool,
    ):
        """
        Обрабатывает результат одного запуска

        Args:
            overall_result: Общий результат для добавления информации
            workload_name: Имя workload
            run_num: Номер запуска
            run_config: Конфигурация запуска
            success: Успешность выполнения
            execution_time: Время выполнения
            stdout: Вывод workload
            stderr: Ошибки workload
            is_timeout: Флаг таймаута
        """
        # Создаем результат для run'а
        run_result = self.create_workload_result(
            workload_name=f"{workload_name}_run_{run_num}",
            stdout=stdout,
            stderr=stderr,
            success=success,
            additional_stats={
                "run_number": run_num,
                "run_duration": run_config.get("duration"),
                "run_execution_time": execution_time,
                # Номер итерации
                "iteration_num": run_config.get("iteration_num", 1),
                # Идентификатор потока
                "thread_id": run_config.get("thread_id", ""),
                "iter_prefix_added": run_config.get(
                    "iter_prefix_added", False
                ),  # Флаг, что префикс iter_ уже добавлен
                **run_config,
            },
            is_timeout=is_timeout,
            iteration_number=run_num,
            actual_execution_time=execution_time,
        )

        # Добавляем iteration в общий результат
        overall_result.iterations[run_num] = run_result.iterations[run_num]

        # Накапливаем stdout/stderr
        if overall_result.stdout is None:
            overall_result.stdout = ""
        if overall_result.stderr is None:
            overall_result.stderr = ""

        overall_result.stdout += f"\n=== Run {run_num} ===\n{stdout or ''}"
        overall_result.stderr += f"\n == = Run {run_num} stderr == =\n{
            stderr or ''}"

    def _analyze_execution_results(
        self,
        overall_result,
        successful_runs: int,
        total_runs: int,
    ):
        """
        Анализирует результаты выполнения и добавляет ошибки/предупреждения

        Args:
            overall_result: Общий результат для добавления информации
            successful_runs: Количество успешных запусков
            total_runs: Общее количество запусков
            use_iterations: Использовались ли итерации
        """
        # Группируем итерации по их номеру, чтобы определить реальное
        # количество итераций
        iterations_by_number = {}
        threads_by_iteration = {}

        # Анализируем все итерации и группируем их по номеру итерации
        for iter_num, iteration in overall_result.iterations.items():
            # Получаем номер итерации из статистики или из имени
            real_iter_num = None
            node_host = None

            # Проверяем статистику
            if hasattr(iteration, "stats") and iteration.stats:
                for stat_key, stat_value in iteration.stats.items():
                    if isinstance(stat_value, dict):
                        if stat_key == "iteration_info":
                            if "iteration_num" in stat_value:
                                real_iter_num = stat_value["iteration_num"]
                            if "node_host" in stat_value:
                                node_host = stat_value["node_host"]
                        elif "iteration_num" in stat_value:
                            real_iter_num = stat_value["iteration_num"]

            # Если не нашли в статистике, пробуем извлечь из имени
            if real_iter_num is None and hasattr(
                    iteration, "name") and iteration.name:
                name_parts = iteration.name.split("_")
                for i, part in enumerate(name_parts):
                    if part == "iter" and i + 1 < len(name_parts):
                        try:
                            real_iter_num = int(name_parts[i + 1])
                            break
                        except (ValueError, IndexError):
                            pass

            # Если все еще не определили, используем номер итерации
            if real_iter_num is None:
                real_iter_num = iter_num

            # Добавляем итерацию в группу по её номеру
            if real_iter_num not in iterations_by_number:
                iterations_by_number[real_iter_num] = []
                threads_by_iteration[real_iter_num] = set()

            iterations_by_number[real_iter_num].append(iteration)
            if node_host:
                threads_by_iteration[real_iter_num].add(node_host)

        # Определяем реальное количество итераций и потоков
        real_iteration_count = len(iterations_by_number)
        failed_iterations = 0

        # Проверяем каждую итерацию на наличие ошибок
        for iter_num, iterations in iterations_by_number.items():
            # Если хотя бы один поток в итерации завершился успешно, считаем
            # итерацию успешной
            iteration_success = any(
                not hasattr(
                    iter_obj, "error_message") or not iter_obj.error_message
                for iter_obj in iterations
            )

            if not iteration_success:
                failed_iterations += 1

        # Формируем сообщение об ошибке или предупреждение
        if failed_iterations == real_iteration_count and real_iteration_count > 0:
            # Все итерации завершились с ошибкой
            threads_info = ""
            if (
                real_iteration_count == 1
                and sum(len(threads) for threads in threads_by_iteration.values()) > 1
            ):
                # Если была только одна итерация с несколькими потоками,
                # указываем это
                thread_count = sum(
                    len(threads) for threads in threads_by_iteration.values()
                )
                threads_info = f" with {thread_count} parallel threads"

            overall_result.add_error(
                f"All {real_iteration_count} iterations{threads_info} failed to execute successfully"
            )
        elif failed_iterations > 0:
            # Некоторые итерации завершились с ошибкой
            overall_result.add_warning(
                f"{failed_iterations} out of {real_iteration_count} iterations failed to execute successfully"
            )

    def _add_execution_statistics(
        self,
        overall_result,
        workload_name: str,
        execution_result: dict,
        additional_stats: dict,
        duration_value: float,
    ):
        """
        Собирает и добавляет статистику выполнения

        Args:
            overall_result: Общий результат для добавления статистики
            workload_name: Имя workload
            execution_result: Результаты выполнения
            additional_stats: Дополнительная статистика
            duration_value: Время выполнения в секундах
        """
        successful_runs = execution_result["successful_runs"]
        total_runs = execution_result["total_runs"]
        total_execution_time = execution_result["total_execution_time"]

        # Группируем итерации по их номеру для определения реального количества
        # итераций и потоков
        iterations_by_number = {}
        threads_by_iteration = {}

        # Анализируем все итерации и группируем их по номеру итерации
        for iter_num, iteration in overall_result.iterations.items():
            # Получаем номер итерации из статистики или из имени
            real_iter_num = None
            node_host = None
            # actual_time = None

            # Проверяем статистику
            if hasattr(iteration, "stats") and iteration.stats:
                for stat_key, stat_value in iteration.stats.items():
                    if isinstance(stat_value, dict):
                        if stat_key == "iteration_info":
                            if "iteration_num" in stat_value:
                                real_iter_num = stat_value["iteration_num"]
                            if "node_host" in stat_value:
                                node_host = stat_value["node_host"]
                            # if "actual_execution_time" in stat_value:
                                # actual_time = stat_value["actual_execution_time"]
                        elif "iteration_num" in stat_value:
                            real_iter_num = stat_value["iteration_num"]
                        elif "chunk_num" in stat_value:  # Для обратной совместимости
                            real_iter_num = stat_value["chunk_num"]

            # Если не нашли в статистике, пробуем извлечь из имени
            if real_iter_num is None and hasattr(
                    iteration, "name") and iteration.name:
                name_parts = iteration.name.split("_")
                for i, part in enumerate(name_parts):
                    if part == "iter" and i + 1 < len(name_parts):
                        try:
                            real_iter_num = int(name_parts[i + 1])
                            break
                        except (ValueError, IndexError):
                            pass

            # Если все еще не определили, используем номер итерации
            if real_iter_num is None:
                real_iter_num = iter_num

            # Добавляем итерацию в группу по её номеру
            if real_iter_num not in iterations_by_number:
                iterations_by_number[real_iter_num] = []
                threads_by_iteration[real_iter_num] = set()

            iterations_by_number[real_iter_num].append(iteration)
            if node_host:
                threads_by_iteration[real_iter_num].add(node_host)

        # Определяем реальное количество итераций и потоков
        real_iteration_count = len(iterations_by_number)
        total_thread_count = sum(
            len(threads) for threads in threads_by_iteration.values()
        )

        # Считаем успешные итерации
        successful_iterations = 0
        for iter_num, iterations in iterations_by_number.items():
            # Если хотя бы один поток в итерации завершился успешно, считаем
            # итерацию успешной
            iteration_success = any(
                not hasattr(
                    iter_obj, "error_message") or not iter_obj.error_message
                for iter_obj in iterations
            )

            if iteration_success:
                successful_iterations += 1

        # Вычисляем фактическое время выполнения как среднее по всем итерациям
        actual_execution_times = []
        for iter_list in iterations_by_number.values():
            for iteration in iter_list:
                if hasattr(iteration, "time") and iteration.time is not None:
                    actual_execution_times.append(iteration.time)
                elif hasattr(iteration, "stats") and iteration.stats:
                    for stat_key, stat_value in iteration.stats.items():
                        if (
                            isinstance(stat_value, dict)
                            and "actual_execution_time" in stat_value
                        ):
                            actual_execution_times.append(
                                stat_value["actual_execution_time"]
                            )

        # Вычисляем среднее фактическое время выполнения
        avg_actual_time = (
            sum(actual_execution_times) / len(actual_execution_times)
            if actual_execution_times
            else duration_value
        )

        # Базовая статистика
        stats = {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": total_runs - successful_runs,
            "total_execution_time": total_execution_time,
            "planned_duration": duration_value,
            "actual_duration": avg_actual_time,  # Добавляем фактическое время выполнения
            "success_rate": successful_runs / total_runs if total_runs > 0 else 0,
            "total_iterations": real_iteration_count,
            "successful_iterations": successful_iterations,
            "failed_iterations": real_iteration_count - successful_iterations,
            "total_threads": total_thread_count,
            "avg_threads_per_iteration": (
                total_thread_count / real_iteration_count
                if real_iteration_count > 0
                else 0
            ),
        }

        # Добавляем дополнительную статистику
        if additional_stats:
            stats.update(additional_stats)

        # Добавляем всю статистику в результат
        for key, value in stats.items():
            overall_result.add_stat(workload_name, key, value)

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
            self._analyze_execution_results(
                overall_result, successful_runs, total_runs
            )

            # Собираем и добавляем статистику
            self._add_execution_statistics(
                overall_result,
                'parallel_run',
                execution_result,
                additional_stats,
                duration_value,
            )

            # Финальная обработка с диагностикой (подготавливает данные для выгрузки)
            overall_result.workload_start_time = execution_result["workload_start_time"]
            self.process_workload_result_with_diagnostics(
                overall_result, 'parallel_run', False, execution_result
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
        check_scheme: bool = True,
        execution_result = None
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
            start_time = result.start_time if result.start_time else end_time - 1

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

