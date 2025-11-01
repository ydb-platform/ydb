import traceback
import uuid
import allure
import logging
import time as time_module
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from ydb.tests.library.stability.aggregate_results import process_single_run_result
from ydb.tests.library.stability.deploy import StressUtilDeployer

from ydb.tests.library.stability import deploy
from ydb.tests.olap.lib.utils import external_param_is_true
from ydb.tests.olap.lib.remote_execution import execute_command


class StressRunExecutor:
    def __init__(self):
        self._ignore_stderr_content = external_param_is_true('ignore_stderr_content')

    def __substitute_variables_in_template(
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

    def execute_stress_runs(
        self,
        stress_deployer: StressUtilDeployer,
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
                    deploy.nemesis_started = True
                    logging.info("Nemesis flag set to True - will start in 15 seconds")

                    nemesis_thread = threading.Thread(
                        target=stress_deployer.delayed_nemesis_start,
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
                        run_config_copy["duration"] = round(run_duration)
                        run_config_copy["node_role"] = node['node'].role
                        run_config_copy["thread_id"] = (
                            node_host  # Идентификатор потока - хост ноды
                        )

                        # Выполняем один run
                        success, execution_time, stdout, stderr, is_timeout = (
                            self._execute_single_workload_run(
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
                            "start_time": start_time,
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
                        process_single_run_result(
                            overall_result,
                            node_result["stress_name"],
                            global_run_num,
                            run_result["run_config"],
                            run_result["success"],
                            run_result["execution_time"],
                            run_result["start_time"],
                            run_result["stdout"],
                            run_result["stderr"],
                            run_result["is_timeout"],
                            self._ignore_stderr_content
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
        command_args = self.__substitute_variables_in_template(
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

                    if self._ignore_stderr_content:
                        success = not is_timeout
                    else:
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
