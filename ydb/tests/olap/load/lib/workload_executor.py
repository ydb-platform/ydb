import allure
import logging
import os
import yatest.common
from time import time

from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.remote_execution import execute_command, deploy_binaries_to_hosts
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper

# Импортируем LoadSuiteBase чтобы наследоваться от него
from ydb.tests.olap.load.lib.conftest import LoadSuiteBase


class WorkloadTestBase(LoadSuiteBase):
    """
    Базовый класс для workload тестов с общей функциональностью
    """

    # Переопределяемые атрибуты в наследниках
    workload_binary_name: str = None  # Имя бинарного файла workload
    workload_env_var: str = None      # Переменная окружения с путем к бинарному файлу
    binaries_deploy_path: str = '/tmp/stress_binaries/'  # Путь для деплоя бинарных файлов
    timeout: float = 1800.  # Таймаут по умолчанию

    @classmethod
    def do_setup_class(cls) -> None:
        """Сохраняем время начала setup для использования в start_time"""
        cls._setup_start_time = time()

    @classmethod
    def do_teardown_class(cls):
        """
        Общая очистка для workload тестов.
        Останавливает процессы workload на всех нодах кластера.
        """
        if cls.workload_binary_name is None:
            logging.warning(f"workload_binary_name not set for {cls.__name__}, skipping process cleanup")
            return

        logging.info(f"Starting {cls.__name__} teardown: stopping workload processes")

        try:
            # Используем метод из LoadSuiteBase напрямую через наследование
            cls.kill_workload_processes(
                process_names=[cls.binaries_deploy_path + cls.workload_binary_name],
                target_dir=cls.binaries_deploy_path
            )
        except Exception as e:
            logging.error(f"Error during teardown: {e}")

    def create_workload_result(self, workload_name: str, stdout: str, stderr: str,
                               success: bool, additional_stats: dict = None, is_timeout: bool = False,
                               iteration_number: int = 0, actual_execution_time: float = None
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
        logging.info(f"Input parameters - success: {success}, stdout length: {len(str(stdout))}, stderr length: {len(str(stderr))}")
        if stdout:
            logging.info(f"Stdout preview (first 200 chars): {str(stdout)[:200]}")
        if stderr:
            logging.info(f"Stderr preview (first 200 chars): {str(stderr)[:200]}")

        # Анализируем результаты выполнения
        error_found = False

        # Проверяем на timeout сначала (это warning, не error)
        if is_timeout:
            result.add_warning(f"Workload execution timed out. stdout: {stdout}, stderr: {stderr}")
        else:
            # Проверяем явные ошибки (только если не timeout)
            if not success:
                result.add_error(f"Workload execution failed. stderr: {stderr}")
                error_found = True
            elif "error" in str(stderr).lower():
                result.add_error(f"Error detected in stderr: {stderr}")
                error_found = True
            elif self._has_real_error_in_stdout(str(stdout)):
                result.add_warning(f"Error detected in stdout: {stdout}")
                error_found = True

        # Проверяем предупреждения
        if ("warning: permanently added" not in str(stderr).lower() and "warning" in str(stderr).lower()):
            result.add_warning(f"Warning in stderr: {stderr}")

        # Добавляем информацию о выполнении в iterations
        iteration = YdbCliHelper.Iteration()
        iteration.time = actual_execution_time if actual_execution_time is not None else self.timeout

        if is_timeout:
            # Для timeout используем более конкретное сообщение
            iteration.error_message = "Workload execution timed out"
        elif error_found:
            # Устанавливаем ошибку в iteration для consistency
            iteration.error_message = result.error_message

        result.iterations[iteration_number] = iteration

        # Добавляем базовую статистику
        result.add_stat(workload_name, "execution_time", self.timeout)
        # Timeout не считается неуспешным выполнением, это warning
        result.add_stat(workload_name, "workload_success", (success and not error_found) or is_timeout)
        result.add_stat(workload_name, "success_flag", success)  # Исходный флаг успеха

        # Добавляем дополнительную статистику если есть
        if additional_stats:
            for key, value in additional_stats.items():
                result.add_stat(workload_name, key, value)

        logging.info(f"Workload result created - final success: {result.success}, error_message: {result.error_message}")

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
            "error responses count:",           # статистика ответов
            "_error responses count:",          # например scheme_error responses count
            "error_responses_count:",           # альтернативный формат
            "errorresponsescount:",             # без разделителей
            "errors encountered:",              # может быть частью статистики
            "total errors:",                    # общая статистика
            "error rate:",                      # метрика частоты ошибок
            "error percentage:",                # процент ошибок
            "error count:",                     # счетчик ошибок в статистике
            "scheme_error responses count:",    # конкретный пример из статистики
            "aborted responses count:",         # другие типы ответов из статистики
            "precondition_failed responses count:",  # еще один тип из статистики
            "error responses:",                 # краткий формат
            "eventkind:",                       # статистика по событиям
            "success responses count:",         # для контекста успешных ответов
            "responses count:",                 # общий паттерн счетчиков ответов
        ]

        # Проверяем, есть ли слово "error" в контексте, который НЕ является ложным срабатыванием
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
            is_false_positive = any(pattern in context for pattern in false_positive_patterns)

            if not is_false_positive:
                # Дополнительные проверки на реальные ошибочные сообщения
                real_error_indicators = [
                    "fatal:",                   # "Fatal: something went wrong"
                    "error:",                   # "Error: something went wrong"
                    "error occurred",           # "An error occurred"
                    "error while",              # "Error while processing"
                    "error during",             # "Error during execution"
                    "fatal error",              # "Fatal error"
                    "runtime error",            # "Runtime error"
                    "execution error",          # "Execution error"
                    "internal error",           # "Internal error"
                    "connection error",         # "Connection error"
                    "timeout error",            # "Timeout error"
                    "failed with error",        # "Operation failed with error"
                    "exceptions must derive from baseexception",  # Python exception error
                ]

                # Если найден реальный индикатор ошибки, возвращаем True
                if any(indicator in context for indicator in real_error_indicators):
                    return True

        return False

    def execute_workload_test(self, workload_name: str, command_args: str, duration_value: float = None, additional_stats: dict = None, use_chunks: bool = False, duration_param: str = "--duration"):
        """
        Выполняет полный цикл workload теста

        Args:
            workload_name: Имя workload для отчетов
            command_args: Аргументы командной строки (может быть шаблоном при use_chunks=True)
            duration_value: Время выполнения в секундах (если None, используется self.timeout)
            additional_stats: Дополнительная статистика
            use_chunks: Использовать ли разбивку на чанки для повышения надежности
            duration_param: Параметр для передачи времени выполнения
        """
        if duration_value is None:
            duration_value = self.timeout

        return self._execute_workload_with_deployment(
            workload_name=workload_name,
            command_args_template=command_args,
            duration_param=duration_param,
            duration_value=duration_value,
            additional_stats=additional_stats,
            use_chunks=use_chunks
        )

    def _execute_workload_with_deployment(self, workload_name: str, command_args_template: str, duration_param: str, duration_value: float, additional_stats: dict = None, use_chunks: bool = False):
        """
        Базовый метод для выполнения workload с deployment и повторными запусками
        """
        logging.info(f"=== Starting workload execution for {workload_name} ===")

        # ФАЗА 1: ПОДГОТОВКА
        preparation_result = self._prepare_workload_execution(
            workload_name, duration_value, use_chunks, duration_param
        )

        # ФАЗА 2: ВЫПОЛНЕНИЕ
        execution_result = self._execute_workload_runs(
            workload_name, command_args_template, duration_param,
            use_chunks, preparation_result
        )

        # ФАЗА 3: РЕЗУЛЬТАТЫ
        final_result = self._finalize_workload_results(
            workload_name, execution_result, additional_stats, duration_value, use_chunks
        )

        logging.info(f"=== Workload test completed for {workload_name} ===")
        return final_result

    def _prepare_workload_execution(self, workload_name: str, duration_value: float, use_chunks: bool, duration_param: str):
        # """ФАЗА 1: Подготовка к выполнению workload"""
        with allure.step('Phase 1: Prepare workload execution'):
            logging.info(f"Preparing execution: Duration={duration_value}s, chunks={use_chunks}, param={duration_param}")

            # Сохраняем состояние нод для диагностики
            self.save_nodes_state()

            # Выполняем deploy
            deployed_binary_path, target_node = self._deploy_workload_binary(workload_name)

            # Создаем план выполнения
            execution_plan = (self._create_chunks_plan(duration_value) if use_chunks else self._create_single_run_plan(duration_value))

            # Инициализируем результат
            overall_result = YdbCliHelper.WorkloadRunResult()
            overall_result.start_time = time()

            logging.info(f"Preparation completed: {len(execution_plan)} runs planned")

            return {
                'deployed_binary_path': deployed_binary_path,
                'target_node': target_node,
                'execution_plan': execution_plan,
                'overall_result': overall_result,
                'workload_start_time': time()
            }

    def _execute_workload_runs(self, workload_name: str, command_args_template: str, duration_param: str, use_chunks: bool, preparation_result: dict):
        # """ФАЗА 2: Выполнение workload runs"""
        with allure.step('Phase 2: Execute workload runs'):
            deployed_binary_path = preparation_result['deployed_binary_path']
            target_node = preparation_result['target_node']
            execution_plan = preparation_result['execution_plan']
            overall_result = preparation_result['overall_result']

            successful_runs = 0
            total_execution_time = 0
            workload_start_time = None

            # Выполняем план
            for run_num, run_config in enumerate(execution_plan, 1):
                # Запоминаем время начала первого workload run
                if run_num == 1:
                    workload_start_time = time()

                run_name = (f"{workload_name}_chunk_{run_config['chunk_num']}" if use_chunks else f"{workload_name}_run_{run_num}")

                # Выполняем один run
                success, execution_time, stdout, stderr, is_timeout = self._execute_single_workload_run(
                    deployed_binary_path, target_node, run_name,
                    command_args_template, duration_param, run_config
                )

                # Обрабатываем результат run'а
                self._process_single_run_result(
                    overall_result, workload_name, run_num, run_config,
                    success, execution_time, stdout, stderr, is_timeout
                )

                total_execution_time += execution_time
                if success:
                    successful_runs += 1
                    logging.info(f"Run {run_num} completed successfully")
                else:
                    logging.warning(f"Run {run_num} failed")
                    if not use_chunks:
                        break  # Прерываем при ошибке для одиночного запуска

            logging.info(f"Execution completed: {successful_runs}/{len(execution_plan)} successful")

            return {
                'overall_result': overall_result,
                'successful_runs': successful_runs,
                'total_runs': len(execution_plan),
                'total_execution_time': total_execution_time,
                'workload_start_time': workload_start_time
            }

    def _finalize_workload_results(self, workload_name: str, execution_result: dict, additional_stats: dict, duration_value: float, use_chunks: bool):
        # """ФАЗА 3: Финализация результатов и диагностика"""
        with allure.step('Phase 3: Finalize results and diagnostics'):
            overall_result = execution_result['overall_result']
            successful_runs = execution_result['successful_runs']
            total_runs = execution_result['total_runs']

            # Проверяем состояние схемы
            self._check_scheme_state()

            # Анализируем результаты и добавляем ошибки/предупреждения
            self._analyze_execution_results(overall_result, successful_runs, total_runs, use_chunks)

            # Собираем и добавляем статистику
            self._add_execution_statistics(
                overall_result, workload_name, execution_result,
                additional_stats, duration_value, use_chunks
            )

            # Финальная обработка с диагностикой
            overall_result.workload_start_time = execution_result['workload_start_time']
            self.process_workload_result_with_diagnostics(overall_result, workload_name, False)

            logging.info(f"Final result: success={overall_result.success}, successful_runs={successful_runs}/{total_runs}")
            return overall_result

    def _process_single_run_result(self, overall_result, workload_name: str, run_num: int, run_config: dict, success: bool, execution_time: float, stdout: str, stderr: str, is_timeout: bool):
        # """Обрабатывает результат одного run'а"""
        # Создаем результат для run'а
        run_result = self.create_workload_result(
            workload_name=f"{workload_name}_run_{run_num}",
            stdout=stdout,
            stderr=stderr,
            success=success,
            additional_stats={
                "run_number": run_num,
                "run_duration": run_config.get('duration'),
                "run_execution_time": execution_time,
                **run_config
            },
            is_timeout=is_timeout,
            iteration_number=run_num,
            actual_execution_time=execution_time
        )

        # Добавляем iteration в общий результат
        overall_result.iterations[run_num] = run_result.iterations[run_num]

        # Накапливаем stdout/stderr
        if overall_result.stdout is None:
            overall_result.stdout = ""
        if overall_result.stderr is None:
            overall_result.stderr = ""

        overall_result.stdout += f"\n=== Run {run_num} ===\n{stdout or ''}"
        overall_result.stderr += f"\n=== Run {run_num} stderr ===\n{stderr or ''}"

    def _analyze_execution_results(self, overall_result, successful_runs: int, total_runs: int, use_chunks: bool):
        # """Анализирует результаты выполнения и добавляет ошибки/предупреждения"""
        if successful_runs == 0:
            overall_result.add_error(f"All {total_runs} runs failed to execute successfully")
        elif successful_runs < total_runs and use_chunks:
            overall_result.add_warning(f"Only {successful_runs}/{total_runs} runs completed successfully")

    def _add_execution_statistics(self, overall_result, workload_name: str, execution_result: dict, additional_stats: dict, duration_value: float, use_chunks: bool):
        # """Собирает и добавляет статистику выполнения"""
        successful_runs = execution_result['successful_runs']
        total_runs = execution_result['total_runs']
        total_execution_time = execution_result['total_execution_time']

        # Базовая статистика
        stats = {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": total_runs - successful_runs,
            "total_execution_time": total_execution_time,
            "planned_duration": duration_value,
            "success_rate": successful_runs / total_runs if total_runs > 0 else 0,
            "use_chunks": use_chunks
        }

        # Добавляем дополнительную статистику
        if additional_stats:
            stats.update(additional_stats)

        # Добавляем всю статистику в результат
        for key, value in stats.items():
            overall_result.add_stat(workload_name, key, value)

    def _deploy_workload_binary(self, workload_name: str):
        """Выполняет deploy workload binary и возвращает путь к бинарному файлу и target node"""
        with allure.step('Deploy workload binary'):
            import yatest.common
            import os

            logging.info(f"Starting deployment for {workload_name}")

            # Получаем бинарный файл
            with allure.step('Get workload binary'):
                binary_files = [yatest.common.binary_path(os.getenv(self.workload_env_var))]
                allure.attach(f"Environment variable: {self.workload_env_var}", 'Binary Configuration', attachment_type=allure.attachment_type.TEXT)
                allure.attach(f"Binary path: {binary_files[0]}", 'Binary Path', attachment_type=allure.attachment_type.TEXT)
                logging.info(f"Binary path resolved: {binary_files[0]}")

            # Получаем ноды кластера и выбираем первую
            with allure.step('Select cluster node'):
                nodes = YdbCluster.get_cluster_nodes()
                if not nodes:
                    raise Exception("No cluster nodes found")

                target_node = nodes[0]
                allure.attach(f"Selected node: {target_node.host}", 'Target Node', attachment_type=allure.attachment_type.TEXT)
                logging.info(f"Selected target node: {target_node.host}")

            # Развертываем бинарный файл
            with allure.step(f'Deploy {self.workload_binary_name} to {target_node.host}'):
                logging.info(f"Starting deployment to {target_node.host}")
                deploy_results = deploy_binaries_to_hosts(
                    binary_files, [target_node.host], self.binaries_deploy_path
                )

                # Проверяем результат развертывания
                binary_result = deploy_results.get(target_node.host, {}).get(self.workload_binary_name, {})
                success = binary_result.get('success', False)

                allure.attach(f"Deployment result: {binary_result}", 'Deployment Details', attachment_type=allure.attachment_type.TEXT)
                logging.info(f"Deployment result: {binary_result}")

                if not success:
                    # Создаем детальное сообщение об ошибке деплоя
                    deploy_error_details = []
                    deploy_error_details.append(f"DEPLOYMENT FAILED: {self.workload_binary_name}")
                    deploy_error_details.append(f"Target host: {target_node.host}")
                    deploy_error_details.append(f"Target directory: {self.binaries_deploy_path}")
                    deploy_error_details.append(f"Binary environment variable: {self.workload_env_var}")
                    deploy_error_details.append(f"Local binary path: {binary_files[0]}")

                    # Детали результата деплоя
                    if 'error' in binary_result:
                        deploy_error_details.append(f"Deploy error: {binary_result['error']}")

                    # Показываем полный результат деплоя для диагностики
                    deploy_error_details.append("\nFull deployment result:")
                    for key, value in binary_result.items():
                        deploy_error_details.append(f"  {key}: {value}")

                    # Информация о хосте
                    deploy_error_details.append("\nHost details:")
                    deploy_error_details.append(f"  Host: {target_node.host}")
                    deploy_error_details.append(f"  Role: {target_node.role}")
                    deploy_error_details.append(f"  Start time: {target_node.start_time}")

                    detailed_deploy_error = "\n".join(deploy_error_details)

                    logging.error(detailed_deploy_error)
                    raise Exception(detailed_deploy_error)

                deployed_binary_path = binary_result['path']
                logging.info(f"Binary deployed successfully to: {deployed_binary_path}")

                return deployed_binary_path, target_node

    def _create_chunks_plan(self, total_duration: float):
        """Создает план выполнения с чанками"""
        # Вычисляем размер чанка: минимум 100 сек, максимум 1 час
        chunk_size = min(max(total_duration // 10, 100), 3600)
        # Если общее время меньше 200 сек - используем один чанк
        if total_duration < 200:
            chunk_size = total_duration

        chunks = []
        remaining_time = total_duration
        chunk_num = 1

        while remaining_time > 0:
            current_chunk_size = min(chunk_size, remaining_time)
            chunks.append({
                'chunk_num': chunk_num,
                'duration': current_chunk_size,
                'start_offset': total_duration - remaining_time
            })
            remaining_time -= current_chunk_size
            chunk_num += 1

        logging.info(f"Created {len(chunks)} chunks with size {chunk_size}s each")
        return chunks

    def _create_single_run_plan(self, total_duration: float):
        """Создает план для одиночного выполнения"""
        return [{'duration': total_duration, 'run_type': 'single'}]

    def _execute_single_workload_run(self, deployed_binary_path: str, target_node, run_name: str, command_args_template: str, duration_param: str, run_config: dict):
        """Выполняет один запуск workload"""

        # Формируем команду
        if duration_param is None:
            # Используем command_args_template как есть (для обратной совместимости)
            command_args = command_args_template
        else:
            # Добавляем duration параметр
            command_args = f"{command_args_template} {duration_param} {run_config['duration']}"

        run_start_time = time()

        try:
            with allure.step(f'Execute {run_name}'):
                allure.attach(f"Run config: {run_config}", 'Run Info', attachment_type=allure.attachment_type.TEXT)
                allure.attach(command_args, 'Command Arguments', attachment_type=allure.attachment_type.TEXT)
                allure.attach(f"Target host: {target_node.host}", 'Execution Target', attachment_type=allure.attachment_type.TEXT)

                # Формируем и выполняем команду
                with allure.step('Execute workload command'):
                    # Отключаем буферизацию для гарантии захвата вывода
                    cmd = f"stdbuf -o0 -e0 {deployed_binary_path} {command_args}"

                    run_timeout = run_config['duration'] + 150  # Добавляем буфер для завершения

                    allure.attach(cmd, 'Full Command', attachment_type=allure.attachment_type.TEXT)
                    allure.attach(f"Timeout: {int(run_timeout)}s", 'Execution Timeout', attachment_type=allure.attachment_type.TEXT)

                    execution_result = execute_command(
                        target_node.host, cmd,
                        raise_on_error=False,
                        timeout=int(run_timeout),
                        raise_on_timeout=False
                    )

                    stdout = execution_result.stdout
                    stderr = execution_result.stderr
                    is_timeout = execution_result.is_timeout

                    # Прикрепляем результаты выполнения команды
                    if stdout:
                        allure.attach(stdout, 'Command Stdout', attachment_type=allure.attachment_type.TEXT)
                    else:
                        allure.attach("(empty)", 'Command Stdout', attachment_type=allure.attachment_type.TEXT)

                    if stderr:
                        allure.attach(stderr, 'Command Stderr', attachment_type=allure.attachment_type.TEXT)
                    else:
                        allure.attach("(empty)", 'Command Stderr', attachment_type=allure.attachment_type.TEXT)

                    # success=True только если stderr пустой (исключая SSH warnings) И нет timeout
                    success = not bool(stderr.strip()) and not is_timeout

                    execution_time = time() - run_start_time

                    logging.info(f"{run_name} completed in {execution_time:.1f}s, success: {success}, timeout: {is_timeout}")

                    return success, execution_time, stdout, stderr, is_timeout

        except Exception as e:
            execution_time = time() - run_start_time
            error_msg = f"Exception in {run_name}: {e}"
            logging.error(error_msg)
            return False, execution_time, "", error_msg, False

    def _check_scheme_state(self):
        """Проверяет состояние схемы базы данных"""
        with allure.step('Checking scheme state'):
            try:

                ydb_cli_path = yatest.common.binary_path(os.getenv('YDB_CLI_BINARY'))
                execution = yatest.common.execute(
                    [ydb_cli_path, '--endpoint', f'{YdbCluster.ydb_endpoint}',
                     '--database', f'/{YdbCluster.ydb_database}',
                     "scheme", "ls", "-lR"],
                    wait=True,
                    check_exit_code=False
                )
                scheme_stdout = execution.std_out.decode('utf-8') if execution.std_out else ""
                scheme_stderr = execution.std_err.decode('utf-8') if execution.std_err else ""
            except Exception as e:
                scheme_stdout = ""
                scheme_stderr = str(e)

            allure.attach(scheme_stdout, 'Scheme state stdout', allure.attachment_type.TEXT)
            if scheme_stderr:
                allure.attach(scheme_stderr, 'Scheme state stderr', allure.attachment_type.TEXT)
            logging.info(f'scheme stdout: {scheme_stdout}')
            if scheme_stderr:
                logging.warning(f'scheme stderr: {scheme_stderr}')
