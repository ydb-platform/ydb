import allure
import logging
import os
import time as time_module
import uuid
import yatest.common
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.remote_execution import (
    execute_command,
    deploy_binaries_to_hosts,
    copy_file,
)
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import get_external_param

# Импортируем LoadSuiteBase чтобы наследоваться от него
from ydb.tests.olap.load.lib.conftest import LoadSuiteBase


class WorkloadTestBase(LoadSuiteBase):
    """
    Базовый класс для workload тестов с общей функциональностью
    """

    # Переопределяемые атрибуты в наследниках
    workload_binary_name: str = None  # Имя бинарного файла workload
    workload_env_var: str = None  # Переменная окружения с путем к бинарному файлу
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
            if not cls.workload_binary_name:
                logging.warning(
                    f"workload_binary_name not set for {
                        cls.__name__}, skipping process cleanup"
                )
                allure.attach(
                    f"workload_binary_name not set for {
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
                cls.kill_workload_processes(
                    process_names=[
                        cls.binaries_deploy_path +
                        cls.workload_binary_name],
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

    @classmethod
    def _manage_nemesis(
        cls,
        enable_nemesis: bool,
        operation_context: str = None,
        existing_log: list = None,
    ):
        """
        Управляет сервисом nemesis на всех уникальных хостах кластера (параллельное выполнение)

        Args:
            enable_nemesis: True для запуска, False для остановки
            operation_context: Контекст операции для логирования
            existing_log: Существующий лог для добавления информации

        Returns:
            Список строк с логом операции
        """
        # Создаем сводный лог для Allure
        nemesis_log = existing_log if existing_log is not None else []

        try:
            # Получаем все уникальные хосты кластера
            nodes = YdbCluster.get_cluster_nodes()
            unique_hosts = set(node.host for node in nodes)

            if enable_nemesis:
                action = "restart"
                action_name = "Starting"
                logging.info(
                    f"Starting nemesis on {
                        len(unique_hosts)} hosts in parallel"
                )

                # Деплоим бинарный файл nemesis на все ноды
                nemesis_log.append(
                    f"Deploying nemesis binary to {len(unique_hosts)} hosts"
                )

                # Получаем путь к бинарному файлу nemesis
                nemesis_binary_path = os.getenv("NEMESIS_BINARY")
                if nemesis_binary_path:
                    nemesis_binary = yatest.common.binary_path(
                        nemesis_binary_path)
                else:
                    # Используем путь по умолчанию
                    nemesis_binary = yatest.common.binary_path(
                        "ydb/tests/tools/nemesis/driver/nemesis"
                    )

                nemesis_log.append(f"Nemesis binary path: {nemesis_binary}")

                # Создаем сводный лог для Allure для файловых операций
                file_ops_log = []
                file_ops_log.append(
                    f"Preparing nemesis configuration and binary on {
                        len(unique_hosts)} hosts in parallel"
                )

                # Добавляем информацию о времени операций
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                file_ops_log.append(f"Time: {current_time}")

                # Деплоим бинарный файл nemesis используя
                # deploy_binaries_to_hosts
                nemesis_binaries = [nemesis_binary]
                nemesis_deploy_path = "/Berkanavt/nemesis/bin/"

                file_ops_log.append(
                    f"Deploying nemesis binary to {len(unique_hosts)} hosts"
                )
                deploy_results = deploy_binaries_to_hosts(
                    nemesis_binaries, list(unique_hosts), nemesis_deploy_path
                )

                # Анализируем результаты деплоя
                successful_deploys = 0
                failed_deploys = 0
                deploy_errors = []

                for host in unique_hosts:
                    binary_result = deploy_results.get(
                        host, {}).get("nemesis", {})
                    success = binary_result.get("success", False)

                    if success:
                        successful_deploys += 1
                        file_ops_log.append(
                            f"  {host}: Binary deployed successfully to {
                                binary_result['path']}"
                        )
                    else:
                        failed_deploys += 1
                        error = binary_result.get("error", "Unknown error")
                        deploy_errors.append(f"{host}: {error}")
                        file_ops_log.append(
                            f"  {host}: Deployment failed - {error}")

                file_ops_log.append("\n--- Binary Deployment Summary ---")
                file_ops_log.append(
                    f"Successful deployments: {successful_deploys}/{
                        len(unique_hosts)}"
                )
                file_ops_log.append(
                    f"Failed deployments: {failed_deploys}/{len(unique_hosts)}"
                )

                if deploy_errors:
                    file_ops_log.append("\nDeployment errors:")
                    for error in deploy_errors:
                        file_ops_log.append(f"- {error}")

                # Функция для выполнения файловых операций на одном хосте
                def prepare_nemesis_config(host):
                    host_log = []
                    host_log.append(f"\n--- {host} ---")
                    logging.info(
                        f"Starting nemesis config preparation for {host}")

                    try:
                        # 1. Устанавливаем права на выполнение для nemesis
                        chmod_cmd = "sudo chmod +x /Berkanavt/nemesis/bin/nemesis"
                        chmod_result = execute_command(
                            host=host, cmd=chmod_cmd, raise_on_error=False
                        )

                        chmod_stderr = (
                            chmod_result.stderr if chmod_result.stderr else ""
                        )
                        if chmod_stderr and "error" in chmod_stderr.lower():
                            error_msg = f"Error setting executable permissions on {host}: {chmod_stderr}"
                            host_log.append(error_msg)
                            return {
                                "host": host,
                                "success": False,
                                "error": error_msg,
                                "log": host_log,
                            }
                        else:
                            host_log.append(
                                "Set executable permissions for nemesis")

                        # 2. Удаляем cluster.yaml
                        delete_cmd = "sudo rm -f /Berkanavt/kikimr/cfg/cluster.yaml"
                        delete_result = execute_command(
                            host=host, cmd=delete_cmd, raise_on_error=False
                        )

                        delete_stderr = (
                            delete_result.stderr if delete_result.stderr else ""
                        )
                        if delete_stderr and "error" in delete_stderr.lower():
                            error_msg = f"Error deleting cluster.yaml on {host}: {delete_stderr}"
                            host_log.append(error_msg)
                            return {
                                "host": host,
                                "success": False,
                                "error": error_msg,
                                "log": host_log,
                            }
                        else:
                            host_log.append("Deleted cluster.yaml")

                        # 3. Копируем config.yaml в cluster.yaml
                        copy_result = cls._copy_cluster_config(host, host_log)
                        if not copy_result["success"]:
                            return copy_result

                        logging.info(f"Completed nemesis config preparation for {host}")
                        return {"host": host, "success": True, "log": host_log}

                    except Exception as e:
                        error_msg = f"Exception on {host}: {e}"
                        host_log.append(error_msg)
                        logging.error(
                            f"Exception during nemesis config preparation for {host}: {e}"
                        )
                        return {
                            "host": host,
                            "success": False,
                            "error": error_msg,
                            "log": host_log,
                        }

                # Выполняем файловые операции параллельно
                file_ops_start_time = time_module.time()
                success_count = 0
                error_count = 0
                errors = []

                with ThreadPoolExecutor(
                    max_workers=min(len(unique_hosts), 20)
                ) as executor:
                    future_to_host = {
                        executor.submit(prepare_nemesis_config, host): host
                        for host in unique_hosts
                    }

                    for future in as_completed(future_to_host):
                        try:
                            result = future.result()
                            host_log = result["log"]
                            file_ops_log.extend(host_log)

                            if result["success"]:
                                success_count += 1
                            else:
                                error_count += 1
                                errors.append(result["error"])

                        except Exception as e:
                            host = future_to_host[future]
                            error_msg = f"Exception processing {host}: {e}"
                            file_ops_log.append(f"\n--- {host} ---")
                            file_ops_log.append(error_msg)
                            errors.append(error_msg)
                            error_count += 1

                file_ops_time = time_module.time() - file_ops_start_time

                # Добавляем итоговую статистику файловых операций
                file_ops_log.append("\n--- File Operations Summary ---")
                file_ops_log.append(
                    f"Successful hosts: {success_count}/{len(unique_hosts)}"
                )
                file_ops_log.append(
                    f"Failed hosts: {error_count}/{len(unique_hosts)}")
                file_ops_log.append(f"Execution time: {file_ops_time:.2f}s")

                if errors:
                    file_ops_log.append("\nErrors:")
                    for error in errors:
                        file_ops_log.append(f"- {error}")

                # Добавляем сводный лог файловых операций в Allure
                allure.attach(
                    "\n".join(file_ops_log),
                    "Nemesis Config and Binary Preparation (Parallel)",
                    attachment_type=allure.attachment_type.TEXT,
                )
            else:
                action = "stop"
                action_name = "Stopping"
                logging.info(
                    f"Stopping nemesis on {
                        len(unique_hosts)} hosts in parallel"
                )

            # Добавляем в лог информацию об операции
            if operation_context:
                nemesis_log.append(
                    f"{operation_context}: {action_name} nemesis service on {
                        len(unique_hosts)} hosts in parallel"
                )
            else:
                nemesis_log.append(
                    f"{action_name} nemesis service on {
                        len(unique_hosts)} hosts in parallel"
                )

            # Добавляем информацию о времени запуска/остановки
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            nemesis_log.append(f"Time: {current_time}")

            # Функция для выполнения команды сервиса на одном хосте
            def execute_service_command(host):
                host_log = []
                host_log.append(f"\n--- {host} ---")

                try:
                    cmd = f"sudo service nemesis {action}"
                    result = execute_command(
                        host=host, cmd=cmd, raise_on_error=False)

                    stdout = result.stdout if result.stdout else ""
                    stderr = result.stderr if result.stderr else ""

                    if stderr and "error" in stderr.lower():
                        error_msg = f"Error on {host}: {stderr}"
                        host_log.append(error_msg)

                        # Только для ошибок создаем отдельный шаг и лог
                        with allure.step(
                            f"Error {action.lower()}ing nemesis on {host}"
                        ):
                            allure.attach(
                                f"Command: {cmd}\nstdout: {stdout}\nstderr: {stderr}",
                                f"Nemesis {action} error",
                                attachment_type=allure.attachment_type.TEXT,
                            )
                            logging.warning(
                                f"Error during nemesis {action} on {host}: {stderr}"
                            )

                        return {
                            "host": host,
                            "success": False,
                            "error": error_msg,
                            "log": host_log,
                        }
                    else:
                        host_log.append("Success")
                        return {"host": host, "success": True, "log": host_log}

                except Exception as e:
                    error_msg = f"Exception on {host}: {e}"
                    host_log.append(error_msg)
                    return {
                        "host": host,
                        "success": False,
                        "error": error_msg,
                        "log": host_log,
                    }

            # Выполняем команды сервиса параллельно
            service_start_time = time_module.time()
            success_count = 0
            error_count = 0
            errors = []

            with ThreadPoolExecutor(max_workers=min(len(unique_hosts), 20)) as executor:
                future_to_host = {
                    executor.submit(execute_service_command, host): host
                    for host in unique_hosts
                }

                for future in as_completed(future_to_host):
                    try:
                        result = future.result()
                        host_log = result["log"]
                        nemesis_log.extend(host_log)

                        if result["success"]:
                            success_count += 1
                        else:
                            error_count += 1
                            errors.append(result["error"])

                    except Exception as e:
                        host = future_to_host[future]
                        error_msg = f"Exception processing {host}: {e}"
                        nemesis_log.append(f"\n--- {host} ---")
                        nemesis_log.append(error_msg)
                        errors.append(error_msg)
                        error_count += 1

            service_time = time_module.time() - service_start_time

            # Добавляем итоговую статистику
            nemesis_log.append("\n--- Summary ---")
            nemesis_log.append(
                f"Successful hosts: {success_count}/{len(unique_hosts)}")
            nemesis_log.append(
                f"Failed hosts: {error_count}/{len(unique_hosts)}")
            nemesis_log.append(f"Service operations time: {service_time:.2f}s")

            if errors:
                nemesis_log.append("\nErrors:")
                for error in errors:
                    nemesis_log.append(f"- {error}")

            # Устанавливаем флаг запуска nemesis
            if enable_nemesis:
                cls._nemesis_started = True
                nemesis_log.append("Nemesis service started successfully")
            else:
                cls._nemesis_started = False
                nemesis_log.append("Nemesis service stopped successfully")

            # Добавляем сводный лог в Allure
            allure.attach(
                "\n".join(nemesis_log),
                f"Nemesis {action_name} Summary (Parallel)",
                attachment_type=allure.attachment_type.TEXT,
            )

            return nemesis_log

        except Exception as e:
            error_msg = f"Error managing nemesis: {e}"
            logging.error(error_msg)
            allure.attach(
                str(e), "Nemesis Error", attachment_type=allure.attachment_type.TEXT
            )
            return nemesis_log + [error_msg]

    @classmethod
    def _copy_cluster_config(cls, host: str, host_log: list) -> dict:
        """Копирует конфигурацию кластера на хост"""
        logging.info(f"Cluster path for {host}: {cls.cluster_path}")
        logging.info(f"YAML config for {host}: {cls.yaml_config}")

        # Копируем cluster.yaml (если указан cluster_path)
        cluster_result = cls._copy_single_config(
            host, cls.cluster_path, "/Berkanavt/kikimr/cfg/cluster.yaml", 
            "cluster config", None, host_log
        )
        if not cluster_result["success"]:
            return cluster_result

        # Копируем databases.yaml (если указан yaml_config)
        if cls.yaml_config:
            databases_result = cls._copy_single_config(
                host, cls.yaml_config, "/Berkanavt/kikimr/cfg/databases.yaml",
                "databases config", None, host_log
            )
            if not databases_result["success"]:
                return databases_result

        return {"host": host, "success": True, "log": host_log}

    @classmethod
    def _copy_single_config(cls, host: str, config_path: str, remote_path: str, 
                           config_name: str, fallback_source: str, host_log: list) -> dict:
        """Копирует один файл конфигурации"""
        if config_path:
            # Копируем внешний файл
            if not os.path.exists(config_path):
                error_msg = f"{config_name} file does not exist: {config_path}"
                host_log.append(error_msg)
                logging.error(error_msg)
                return {"host": host, "success": False, "error": error_msg, "log": host_log}

            source = config_path
            copy_method = lambda: copy_file(
                local_path=config_path,
                host=host,
                remote_path=remote_path,
                raise_on_error=False
            )
            success_msg = f"Copied external {config_name} from {config_path}"
        elif fallback_source:
            # Используем локальный fallback
            source = fallback_source
            copy_method = lambda: execute_command(
                host=host,
                cmd=f"sudo cp {fallback_source} {remote_path}",
                raise_on_error=False
            )
            success_msg = f"Copied local {config_name}"
        else:
            # Ничего не копируем
            return {"host": host, "success": True, "log": host_log}

        # Выполняем копирование
        host_log.append(f"Copying {config_name} from {source}")
        result = copy_method()
        
        # Проверяем результат
        if config_path and not result:
            error_msg = f"Failed to copy {config_name} to {remote_path} on {host}"
            host_log.append(error_msg)
            return {"host": host, "success": False, "error": error_msg, "log": host_log}
        elif not config_path and result.stderr and "error" in result.stderr.lower():
            error_msg = f"Error copying {config_name} on {host}: {result.stderr}"
            host_log.append(error_msg)
            return {"host": host, "success": False, "error": error_msg, "log": host_log}

        host_log.append(success_msg)
        logging.info(f"Successfully copied {config_name} to {host}")
        return {"host": host, "success": True, "log": host_log}

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
                f"Stdout preview (first 200 chars): {
                    str(stdout)[
                        :200]}")
        if stderr:
            logging.info(
                f"Stderr preview (first 200 chars): {
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

    def execute_workload_test(
        self,
        workload_name: str,
        command_args: str,
        duration_value: float = None,
        additional_stats: dict = None,
        use_chunks: bool = False,
        duration_param: str = "--duration",
        nemesis: bool = False,
        nodes_percentage: int = 100,
    ):
        """
        Выполняет полный цикл workload теста

        Args:
            workload_name: Имя workload для отчетов
            command_args: Аргументы командной строки (может быть шаблоном при use_iterations=True)
            duration_value: Время выполнения в секундах (если None, используется self.timeout)
            additional_stats: Дополнительная статистика
            use_chunks: Использовать ли разбивку на итерации (устаревший параметр, для обратной совместимости)
            duration_param: Параметр для передачи времени выполнения
            nemesis: Запускать ли сервис nemesis через 15 секунд после начала выполнения workload
            nodes_percentage: Процент нод кластера для запуска workload (от 1 до 100)
        """
        # Для обратной совместимости переименовываем параметр use_chunks в
        # use_iterations
        use_iterations = use_chunks

        if duration_value is None:
            duration_value = self.timeout

        # Проверяем корректность процента нод
        if nodes_percentage < 1 or nodes_percentage > 100:
            raise ValueError(
                f"nodes_percentage должен быть от 1 до 100, получено: {nodes_percentage}"
            )

        # Добавляем информацию о nemesis и nodes_percentage в статистику
        if additional_stats is None:
            additional_stats = {}
        additional_stats["nemesis_enabled"] = nemesis
        additional_stats["nodes_percentage"] = nodes_percentage
        additional_stats["use_iterations"] = use_iterations

        return self._execute_workload_with_deployment(
            workload_name=workload_name,
            command_args_template=command_args,
            duration_param=duration_param,
            duration_value=duration_value,
            additional_stats=additional_stats,
            use_chunks=use_iterations,
            # Передаем параметр под старым именем для обратной совместимости
            nodes_percentage=nodes_percentage,
            nemesis=nemesis,
        )

    def _execute_workload_with_deployment(
        self,
        workload_name: str,
        command_args_template: str,
        duration_param: str,
        duration_value: float,
        additional_stats: dict = None,
        use_chunks: bool = False,
        nodes_percentage: int = 100,
        nemesis: bool = False,
    ):
        """
        Базовый метод для выполнения workload с deployment и повторными запусками

        Args:
            workload_name: Имя workload для отчетов
            command_args_template: Шаблон аргументов командной строки
            duration_param: Параметр для передачи времени выполнения
            duration_value: Время выполнения в секундах
            additional_stats: Дополнительная статистика
            use_chunks: Использовать ли разбивку на чанки
            nodes_percentage: Процент нод кластера для запуска workload
            nemesis: Запускать ли сервис nemesis
        """
        logging.info(
            f"=== Starting workload execution for {workload_name} ===")

        # ФАЗА 1: ПОДГОТОВКА
        preparation_result = self._prepare_workload_execution(
            workload_name, duration_value, use_chunks, duration_param, nodes_percentage
        )

        # ФАЗА 2: ВЫПОЛНЕНИЕ
        execution_result = self._execute_workload_runs(
            workload_name,
            command_args_template,
            duration_param,
            use_chunks,
            preparation_result,
            nemesis,
        )

        # ФАЗА 3: РЕЗУЛЬТАТЫ
        final_result = self._finalize_workload_results(
            workload_name,
            execution_result,
            additional_stats,
            duration_value,
            use_chunks,
        )

        logging.info(f"=== Workload test completed for {workload_name} ===")
        return final_result

    def _prepare_workload_execution(
        self,
        workload_name: str,
        duration_value: float,
        use_chunks: bool,
        duration_param: str,
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
        # Для обратной совместимости переименовываем параметр use_chunks в
        # use_iterations
        use_iterations = use_chunks

        with allure.step("Phase 1: Prepare workload execution"):
            logging.info(
                f"Preparing execution: Duration={duration_value}s, iterations={use_iterations}, "
                f"param={duration_param}, nodes_percentage={nodes_percentage}%, mode=parallel"
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

            # Выполняем deploy на выбранный процент нод
            deployed_nodes = self._deploy_workload_binary(
                workload_name, nodes_percentage
            )

            # Создаем план выполнения
            execution_plan = (
                self._create_chunks_plan(duration_value)
                if use_iterations
                else self._create_single_run_plan(duration_value)
            )

            # Инициализируем результат
            overall_result = YdbCliHelper.WorkloadRunResult()
            overall_result.start_time = time_module.time()

            logging.info(
                f"Preparation completed: {
                    len(execution_plan)} iterations planned on {
                    len(deployed_nodes)} nodes in parallel mode"
            )

            return {
                "deployed_nodes": deployed_nodes,
                "execution_plan": execution_plan,
                "overall_result": overall_result,
                "workload_start_time": time_module.time(),
            }

    def _execute_workload_runs(
        self,
        workload_name: str,
        command_args_template: str,
        duration_param: str,
        use_chunks: bool,
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
        # Для обратной совместимости переименовываем параметр use_chunks в
        # use_iterations
        use_iterations = use_chunks

        with allure.step("Phase 2: Execute workload runs in parallel"):
            deployed_nodes = preparation_result["deployed_nodes"]
            execution_plan = preparation_result["execution_plan"]
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
                f"Execute workload in parallel on {len(deployed_nodes)} nodes"
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

                # Для каждой ноды создаем свой план выполнения
                node_execution_plans = []
                for node in deployed_nodes:
                    # Каждая нода получает копию плана выполнения
                    node_plan = {"node": node, "plan": execution_plan}
                    node_execution_plans.append(node_plan)

                logging.info(
                    f"Starting parallel execution on {
                        len(node_execution_plans)} nodes"
                )

                # Результаты выполнения для каждой ноды
                node_results = []

                # Блокировка для безопасного обновления общих данных из потоков
                threading.Lock()

                # Функция для выполнения workload на одной ноде
                def execute_on_node(node_plan):
                    node = node_plan["node"]
                    plan = node_plan["plan"]
                    node_host = node["node"].host
                    deployed_binary_path = node["binary_path"]

                    node_result = {
                        "node": node,
                        "host": node_host,
                        "successful_runs": 0,
                        "total_runs": len(plan),
                        "runs": [],
                        "total_execution_time": 0,
                    }

                    logging.info(f"Starting execution on node {node_host}")

                    # Выполняем план для этой ноды
                    for run_num, run_config in enumerate(plan, 1):
                        # Формируем имя запуска с учетом ноды и номера итерации
                        iteration_num = run_config.get(
                            "iteration_num", run_num)
                        # Используем формат iter_N без добавления префикса iter_ в _execute_single_workload_run
                        # так как он будет добавлен там
                        run_name = f"{workload_name}_{node_host}_iter_{iteration_num}"

                        # Устанавливаем флаг, что префикс iter_ уже добавлен
                        run_config_copy = run_config.copy()
                        run_config_copy["node_host"] = node_host
                        run_config_copy["node_role"] = node["node"].role
                        run_config_copy["thread_id"] = (
                            node_host  # Идентификатор потока - хост ноды
                        )
                        run_config_copy["iter_prefix_added"] = (
                            True  # Флаг, что префикс iter_ уже добавлен
                        )

                        # Выполняем один run
                        success, execution_time, stdout, stderr, is_timeout = (
                            self._execute_single_workload_run(
                                deployed_binary_path,
                                node["node"],
                                run_name,
                                command_args_template,
                                duration_param,
                                run_config_copy,
                            )
                        )

                        # Сохраняем результат запуска
                        run_result = {
                            "run_num": run_num,
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
                                f"Run {run_num} on {node_host} completed successfully"
                            )
                        else:
                            logging.warning(
                                f"Run {run_num} on {node_host} failed")
                        if not use_iterations:
                            break  # Прерываем при ошибке для одиночного запуска

                    logging.info(
                        f"Execution on {node_host} completed: "
                        f"{node_result['successful_runs']}/{node_result['total_runs']} successful"
                    )

                    return node_result

                # Запускаем параллельное выполнение на всех нодах
                with ThreadPoolExecutor(
                    max_workers=len(node_execution_plans)
                ) as executor:
                    future_to_node = {
                        executor.submit(execute_on_node, node_plan): node_plan
                        for node_plan in node_execution_plans
                    }

                    for future in as_completed(future_to_node):
                        try:
                            node_result = future.result()
                            node_results.append(node_result)
                        except Exception as e:
                            node_plan = future_to_node[future]
                            node_host = node_plan["node"]["node"].host
                            logging.error(
                                f"Error executing on {node_host}: {e}")
                            # Добавляем информацию об ошибке
                            node_results.append(
                                {
                                    "node": node_plan["node"],
                                    "host": node_host,
                                    "error": str(e),
                                    "successful_runs": 0,
                                    "total_runs": len(node_plan["plan"]),
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
                            node_idx * len(execution_plan) +
                            run_result["run_num"]
                        )

                        # Обрабатываем результат запуска
                        self._process_single_run_result(
                            overall_result,
                            workload_name,
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

    def _delayed_nemesis_start(self, delay_seconds: int):
        """
        Запускает nemesis с задержкой после начала выполнения workload

        Args:
            delay_seconds: Задержка в секундах перед запуском nemesis
        """
        try:
            # Создаем лог для Allure
            nemesis_log = []
            start_time = datetime.now()
            nemesis_log.append(
                f"Nemesis scheduled start at {
                    start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            nemesis_log.append(f"Delay: {delay_seconds} seconds")

            logging.info(f"Nemesis will start in {delay_seconds} seconds...")

            # Добавляем информацию о запланированном времени запуска
            planned_start_time = start_time + timedelta(seconds=delay_seconds)
            nemesis_log.append(
                f"Planned start time: {
                    planned_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Добавляем предварительный лог в Allure
            allure.attach(
                "\n".join(nemesis_log),
                "Nemesis Scheduled Start",
                attachment_type=allure.attachment_type.TEXT,
            )

            # Ждем указанное время
            time_module.sleep(delay_seconds)

            # Обновляем лог
            actual_start_time = datetime.now()
            nemesis_log.append(
                f"Actual start time: {
                    actual_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            logging.info(
                f"Starting nemesis after {delay_seconds}s delay at {
                    actual_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            with allure.step(f"Start nemesis after {delay_seconds}s delay"):
                # Запускаем nemesis через общий метод класса
                allure.attach(
                    "\n".join(nemesis_log),
                    "Nemesis Delayed Start Info",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info("Calling _manage_nemesis(True) to start nemesis service")
                self.__class__._manage_nemesis(
                    True, f"Delayed start after {delay_seconds}s", nemesis_log
                )

            logging.info("Nemesis started successfully after delay")

        except Exception as e:
            error_msg = f"Error starting nemesis after delay: {e}"
            logging.error(error_msg)
            allure.attach(
                f"{error_msg}\n\nDelay: {delay_seconds} seconds",
                "Nemesis Delayed Start Error",
                attachment_type=allure.attachment_type.TEXT,
            )

    def _deploy_workload_binary(
            self, workload_name: str, nodes_percentage: int = 100):
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
                binary_files = [
                    yatest.common.binary_path(
                        os.getenv(
                            self.workload_env_var,
                            "ydb/tests/stress/"
                            + self.workload_binary_name
                            + "/"
                            + self.workload_binary_name,
                        )
                    )
                ]
                allure.attach(
                    f"Environment variable: {self.workload_env_var}",
                    "Binary Configuration",
                    attachment_type=allure.attachment_type.TEXT,
                )
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
                        len(selected_nodes)}/{
                        len(unique_nodes)} unique hosts ({nodes_percentage}%)",
                    "Target Hosts",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info(
                    f"Selected {
                        len(selected_nodes)}/{
                        len(unique_nodes)} unique hosts for deployment"
                )

            # Развертываем бинарный файл на выбранных нодах
            with allure.step(
                f"Deploy {
                    self.workload_binary_name} to {
                    len(selected_nodes)} hosts"
            ):
                target_hosts = [node.host for node in selected_nodes]
                logging.info(f"Starting deployment to hosts: {target_hosts}")

                deploy_results = deploy_binaries_to_hosts(
                    binary_files, target_hosts, self.binaries_deploy_path
                )

                # Собираем информацию о результатах деплоя
                deployed_nodes = []
                failed_nodes = []

                for node in selected_nodes:
                    binary_result = deploy_results.get(node.host, {}).get(
                        self.workload_binary_name, {}
                    )
                    success = binary_result.get("success", False)

                    if success:
                        deployed_nodes.append(
                            {"node": node,
                             "binary_path": binary_result["path"]}
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
                        len(deployed_nodes)}/{
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
                        f"DEPLOYMENT FAILED: {self.workload_binary_name}"
                    )
                    deploy_error_details.append(
                        f"Target hosts: {target_hosts}")
                    deploy_error_details.append(
                        f"Target directory: {self.binaries_deploy_path}"
                    )
                    deploy_error_details.append(
                        f"Binary environment variable: {self.workload_env_var}"
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

    def _create_chunks_plan(self, total_duration: float):
        """
        Создает план выполнения с разделением на итерации

        Args:
            total_duration: Общая длительность теста в секундах

        Returns:
            Список итераций с информацией о длительности каждой
        """
        # Вычисляем размер итерации: минимум 100 сек, максимум 1 час
        iteration_size = min(max(total_duration // 10, 100), 3600)
        # Если общее время меньше 200 сек - используем одну итерацию
        if total_duration < 200:
            iteration_size = total_duration

        iterations = []
        remaining_time = total_duration
        iteration_num = 1

        while remaining_time > 0:
            current_iteration_size = min(iteration_size, remaining_time)
            iterations.append(
                {
                    "iteration_num": iteration_num,
                    "duration": current_iteration_size,
                    "start_offset": total_duration - remaining_time,
                }
            )
            remaining_time -= current_iteration_size
            iteration_num += 1

        logging.info(
            f"Created {
                len(iterations)} iterations with size {iteration_size}s each"
        )
        return iterations

    def _create_single_run_plan(self, total_duration: float):
        """
        Создает план для одиночного выполнения (одна итерация)

        Args:
            total_duration: Общая длительность теста в секундах

        Returns:
            Список с одной итерацией
        """
        return [{"duration": total_duration,
                 "run_type": "single", "iteration_num": 1}]

    def _execute_single_workload_run(
        self,
        deployed_binary_path: str,
        target_node,
        run_name: str,
        command_args_template: str,
        duration_param: str,
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

        # Формируем команду
        if duration_param is None:
            # Используем command_args как есть (для обратной совместимости)
            pass
        else:
            # Добавляем duration параметр
            command_args = (
                f"{command_args} {duration_param} {
                    run_config['duration']}"
            )

        # Добавляем информацию об итерации в имя запуска только если префикс
        # еще не добавлен
        if "iteration_num" in run_config and not run_config.get(
            "iter_prefix_added", False
        ):
            # Используем формат iter_N
            run_name = f"{run_name}_iter_{run_config['iteration_num']}"

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
                    f"Target host: {target_node.host}",
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
                        target_node.host,
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
                            execution_time:.1f}s, success: {success}, timeout: {is_timeout}"
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
        node_host = target_node.host
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

    def _check_scheme_state(self):
        """Проверяет состояние схемы базы данных"""
        with allure.step("Checking scheme state"):
            try:

                ydb_cli_path = yatest.common.binary_path(
                    os.getenv("YDB_CLI_BINARY"))
                execution = yatest.common.execute(
                    [
                        ydb_cli_path,
                        "--endpoint",
                        f"{YdbCluster.ydb_endpoint}",
                        "--database",
                        f"/{YdbCluster.ydb_database}",
                        "scheme",
                        "ls",
                        "-lR",
                    ],
                    wait=True,
                    check_exit_code=False,
                )
                scheme_stdout = (
                    execution.std_out.decode(
                        "utf-8") if execution.std_out else ""
                )
                scheme_stderr = (
                    execution.std_err.decode(
                        "utf-8") if execution.std_err else ""
                )
            except Exception as e:
                scheme_stdout = ""
                scheme_stderr = str(e)

            allure.attach(
                scheme_stdout, "Scheme state stdout", allure.attachment_type.TEXT
            )
            if scheme_stderr:
                allure.attach(
                    scheme_stderr, "Scheme state stderr", allure.attachment_type.TEXT
                )
            logging.info(f"scheme stdout: {scheme_stdout}")
            if scheme_stderr:
                logging.warning(f"scheme stderr: {scheme_stderr}")

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
        overall_result.stderr += f"\n=== Run {run_num} stderr ===\n{
            stderr or ''}"

    def _analyze_execution_results(
        self,
        overall_result,
        successful_runs: int,
        total_runs: int,
        use_iterations: bool,
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
        use_iterations: bool,
    ):
        """
        Собирает и добавляет статистику выполнения

        Args:
            overall_result: Общий результат для добавления статистики
            workload_name: Имя workload
            execution_result: Результаты выполнения
            additional_stats: Дополнительная статистика
            duration_value: Время выполнения в секундах
            use_iterations: Использовались ли итерации
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
            "use_iterations": use_iterations,
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
        workload_name: str,
        execution_result: dict,
        additional_stats: dict,
        duration_value: float,
        use_chunks: bool,
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
        # Для обратной совместимости переименовываем параметр use_chunks в
        # use_iterations
        use_iterations = use_chunks

        with allure.step("Phase 3: Finalize results and diagnostics"):
            overall_result = execution_result["overall_result"]
            successful_runs = execution_result["successful_runs"]
            total_runs = execution_result["total_runs"]

            # Проверяем состояние схемы
            self._check_scheme_state()

            # Анализируем результаты и добавляем ошибки/предупреждения
            self._analyze_execution_results(
                overall_result, successful_runs, total_runs, use_iterations
            )

            # Собираем и добавляем статистику
            self._add_execution_statistics(
                overall_result,
                workload_name,
                execution_result,
                additional_stats,
                duration_value,
                use_iterations,
            )

            # Финальная обработка с диагностикой
            overall_result.workload_start_time = execution_result["workload_start_time"]
            self.process_workload_result_with_diagnostics(
                overall_result, workload_name, False, use_node_subcols=True
            )

            logging.info(
                f"Final result: success={
                    overall_result.success}, successful_runs={successful_runs}/{total_runs}"
            )
            return overall_result

    def process_workload_result_with_diagnostics(
        self,
        result: YdbCliHelper.WorkloadRunResult,
        workload_name: str,
        check_scheme: bool = True,
        use_node_subcols: bool = False,
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

            # Проверяем состояние нод и собираем ошибки
            try:
                # Используем метод родительского класса для диагностики нод
                end_time = time_module.time()
                diagnostics_start_time = getattr(
                    result, "workload_start_time", result.start_time
                )
                node_errors = self.check_nodes_diagnostics_with_timing(
                    result, diagnostics_start_time, end_time
                )

            except Exception as e:
                logging.error(f"Error getting nodes state: {e}")
                # Добавляем ошибку в результат
                result.add_warning(f"Error getting nodes state: {e}")

            # Вычисляем время выполнения
            end_time = time_module.time()
            start_time = result.start_time if result.start_time else end_time - 1

            # Добавляем информацию о workload в отчет
            from ydb.tests.olap.lib.allure_utils import allure_test_description

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
                            avg_threads:.1f} threads per iteration"
                    )

            # Создаем отчет
            allure_test_description(
                suite="workload",
                test=workload_name,
                start_time=start_time,
                end_time=end_time,
                addition_table_strings=additional_table_strings,
                node_errors=node_errors,
                workload_result=result,
                workload_params=workload_params,
                use_node_subcols=use_node_subcols,
            )

            # --- ВАЖНО: выставляем nodes_with_issues для корректного fail ---
            stats = result.get_stats(workload_name)
            if stats is not None:
                result.add_stat(
                    workload_name,
                    "nodes_with_issues",
                    len(node_errors))

            # --- Обработка финального статуса (используем методы из базового класса) ---
            self._update_summary_flags(result, workload_name)
            self._handle_final_status(result, workload_name, node_errors)

            # --- Загрузка результатов ---
            self._upload_results(result, workload_name)
            self._upload_results_per_workload_run(result, workload_name)

    def _upload_results(self, result, workload_name):
        """Переопределенный метод для workload тестов с kind='Stability'"""
        stats = result.get_stats(workload_name)
        if stats is not None:
            stats["aggregation_level"] = "aggregate"
            stats["run_id"] = ResultsProcessor.get_run_id()
        end_time = time_module.time()
        ResultsProcessor.upload_results(
            kind="Stability",
            suite=type(self).suite(),
            test=workload_name,
            timestamp=end_time,
            is_successful=result.success,
            statistics=stats,
        )

    def _upload_results_per_workload_run(self, result, workload_name):
        """Переопределенный метод для workload тестов с kind='Stability'"""
        suite = type(self).suite()
        agg_stats = result.get_stats(workload_name)
        nemesis_enabled = agg_stats.get(
            "nemesis_enabled") if agg_stats else None
        run_id = ResultsProcessor.get_run_id()
        for iter_num, iteration in result.iterations.items():
            runs = getattr(iteration, "runs", None) or [iteration]
            for run_idx, run in enumerate(runs):
                if getattr(run, "error_message", None):
                    resolution = "error"
                elif getattr(run, "warning_message", None):
                    resolution = "warning"
                elif hasattr(run, "timeout") and run.timeout:
                    resolution = "timeout"
                else:
                    resolution = "ok"

                stats = {
                    "iteration": iter_num,
                    "run_index": run_idx,
                    "duration": getattr(run, "time", None),
                    "resolution": resolution,
                    "error_message": getattr(run, "error_message", None),
                    "warning_message": getattr(run, "warning_message", None),
                    "nemesis_enabled": nemesis_enabled,
                    "aggregation_level": "per_run",
                    "run_id": run_id,
                }
                ResultsProcessor.upload_results(
                    kind="Stability",
                    suite=suite,
                    test=f"{workload_name}__iter_{iter_num}__run_{run_idx}",
                    timestamp=time_module.time(),
                    is_successful=(resolution == "ok"),
                    duration=stats["duration"],
                    statistics=stats,
                )
