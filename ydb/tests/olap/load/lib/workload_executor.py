import allure
import logging
import os
import yatest.common
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
import time as time_module

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
    _nemesis_started: bool = False  # Флаг для отслеживания запуска nemesis

    @classmethod
    def do_teardown_class(cls):
        """
        Общая очистка для workload тестов.
        Останавливает процессы workload на всех нодах кластера.
        Останавливает nemesis, если он был запущен.
        """
        if not cls.workload_binary_name:
            logging.warning(f"workload_binary_name not set for {cls.__name__}, skipping process cleanup")
            return

        logging.info(f"Starting {cls.__name__} teardown: stopping workload processes")

        try:
            # Используем метод из LoadSuiteBase напрямую через наследование
            cls.kill_workload_processes(
                process_names=[cls.binaries_deploy_path + cls.workload_binary_name],
                target_dir=cls.binaries_deploy_path
            )
            
            # Останавливаем nemesis, если он был запущен
            if getattr(cls, '_nemesis_started', False):
                logging.info("Stopping nemesis service")
                cls._stop_nemesis()
                
        except Exception as e:
            logging.error(f"Error during teardown: {e}")

    @classmethod
    def _stop_nemesis(cls):
        """Останавливает сервис nemesis на всех нодах кластера"""
        try:
            nodes = YdbCluster.get_cluster_nodes()
            unique_hosts = set(node.host for node in nodes)
            
            for host in unique_hosts:
                logging.info(f"Stopping nemesis on {host}")
                execute_command(
                    host=host,
                    cmd="sudo service nemesis stop",
                    raise_on_error=False
                )
            cls._nemesis_started = False
        except Exception as e:
            logging.error(f"Error stopping nemesis: {e}")

    def _manage_nemesis(self, enable_nemesis: bool):
        """
        Управляет сервисом nemesis на всех уникальных хостах кластера
        
        Args:
            enable_nemesis: True для запуска, False для остановки
        """
        if not enable_nemesis:
            return
            
        with allure.step('Manage nemesis service'):
            try:
                # Получаем все уникальные хосты кластера
                nodes = YdbCluster.get_cluster_nodes()
                unique_hosts = set(node.host for node in nodes)
                
                if enable_nemesis:
                    action = "restart"
                    logging.info(f"Starting nemesis on {len(unique_hosts)} hosts")
                else:
                    action = "stop"
                    logging.info(f"Stopping nemesis on {len(unique_hosts)} hosts")
                
                # Выполняем команду на каждом уникальном хосте
                for host in unique_hosts:
                    with allure.step(f'{action.capitalize()} nemesis on {host}'):
                        cmd = f"sudo service nemesis {action}"
                        allure.attach(cmd, 'Nemesis Command', attachment_type=allure.attachment_type.TEXT)
                        
                        result = execute_command(
                            host=host,
                            cmd=cmd,
                            raise_on_error=False
                        )
                        
                        stdout = result.stdout if result.stdout else ""
                        stderr = result.stderr if result.stderr else ""
                        
                        allure.attach(f"stdout: {stdout}\nstderr: {stderr}", 
                                    f'Nemesis {action} result', 
                                    attachment_type=allure.attachment_type.TEXT)
                        
                        if stderr and "error" in stderr.lower():
                            logging.warning(f"Error during nemesis {action} on {host}: {stderr}")
                
                # Устанавливаем флаг запуска nemesis
                if enable_nemesis:
                    self.__class__._nemesis_started = True
                else:
                    self.__class__._nemesis_started = False
                    
            except Exception as e:
                logging.error(f"Error managing nemesis: {e}")
                allure.attach(str(e), 'Nemesis Error', attachment_type=allure.attachment_type.TEXT)

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

        # Добавляем имя итерации для лучшей идентификации
        if additional_stats:
            node_host = additional_stats.get('node_host', '')
            chunk_num = additional_stats.get('chunk_num', iteration_number)
            iteration.name = f"{workload_name}_{node_host}_chunk_{chunk_num}"
            
            # Добавляем статистику о chunk_num и node_host в итерацию
            if not hasattr(iteration, 'stats'):
                iteration.stats = {}
            
            iteration_stats = {
                'chunk_info': {
                    'chunk_num': chunk_num,
                    'node_host': node_host
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

    def execute_workload_test(self, workload_name: str, command_args: str, duration_value: float = None, 
                             additional_stats: dict = None, use_chunks: bool = False, 
                             duration_param: str = "--duration", nemesis: bool = False,
                             nodes_percentage: int = 100):
        """
        Выполняет полный цикл workload теста

        Args:
            workload_name: Имя workload для отчетов
            command_args: Аргументы командной строки (может быть шаблоном при use_chunks=True)
            duration_value: Время выполнения в секундах (если None, используется self.timeout)
            additional_stats: Дополнительная статистика
            use_chunks: Использовать ли разбивку на чанки для повышения надежности
            duration_param: Параметр для передачи времени выполнения
            nemesis: Запускать ли сервис nemesis через 15 секунд после начала выполнения workload
            nodes_percentage: Процент нод кластера для запуска workload (от 1 до 100)
        """
        if duration_value is None:
            duration_value = self.timeout
            
        # Проверяем корректность процента нод
        if nodes_percentage < 1 or nodes_percentage > 100:
            raise ValueError(f"nodes_percentage должен быть от 1 до 100, получено: {nodes_percentage}")
            
        # Добавляем информацию о nemesis и nodes_percentage в статистику
        if additional_stats is None:
            additional_stats = {}
        additional_stats["nemesis_enabled"] = nemesis
        additional_stats["nodes_percentage"] = nodes_percentage

        return self._execute_workload_with_deployment(
            workload_name=workload_name,
            command_args_template=command_args,
            duration_param=duration_param,
            duration_value=duration_value,
            additional_stats=additional_stats,
            use_chunks=use_chunks,
            nodes_percentage=nodes_percentage,
            nemesis=nemesis
        )

    def _execute_workload_with_deployment(self, workload_name: str, command_args_template: str, 
                                         duration_param: str, duration_value: float, 
                                         additional_stats: dict = None, use_chunks: bool = False,
                                         nodes_percentage: int = 100, nemesis: bool = False):
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
        logging.info(f"=== Starting workload execution for {workload_name} ===")

        # ФАЗА 1: ПОДГОТОВКА
        preparation_result = self._prepare_workload_execution(
            workload_name, duration_value, use_chunks, duration_param, nodes_percentage
        )

        # ФАЗА 2: ВЫПОЛНЕНИЕ
        execution_result = self._execute_workload_runs(
            workload_name, command_args_template, duration_param,
            use_chunks, preparation_result, nemesis
        )

        # ФАЗА 3: РЕЗУЛЬТАТЫ
        final_result = self._finalize_workload_results(
            workload_name, execution_result, additional_stats, duration_value, use_chunks
        )

        logging.info(f"=== Workload test completed for {workload_name} ===")
        return final_result

    def _prepare_workload_execution(self, workload_name: str, duration_value: float, 
                                   use_chunks: bool, duration_param: str, 
                                   nodes_percentage: int = 100):
        # """ФАЗА 1: Подготовка к выполнению workload"""
        with allure.step('Phase 1: Prepare workload execution'):
            logging.info(f"Preparing execution: Duration={duration_value}s, chunks={use_chunks}, "
                        f"param={duration_param}, nodes_percentage={nodes_percentage}%, mode=parallel")

            # Сохраняем состояние нод для диагностики
            self.save_nodes_state()

            # Выполняем deploy на выбранный процент нод
            deployed_nodes = self._deploy_workload_binary(workload_name, nodes_percentage)

            # Создаем план выполнения
            execution_plan = (self._create_chunks_plan(duration_value) if use_chunks else self._create_single_run_plan(duration_value))

            # Инициализируем результат
            overall_result = YdbCliHelper.WorkloadRunResult()
            overall_result.start_time = time()

            logging.info(f"Preparation completed: {len(execution_plan)} runs planned on {len(deployed_nodes)} nodes in parallel mode")

            return {
                'deployed_nodes': deployed_nodes,
                'execution_plan': execution_plan,
                'overall_result': overall_result,
                'workload_start_time': time()
            }

    def _execute_workload_runs(self, workload_name: str, command_args_template: str, duration_param: str, use_chunks: bool, preparation_result: dict, nemesis: bool = False):
        # """ФАЗА 2: Параллельное выполнение workload на всех нодах"""
        with allure.step('Phase 2: Execute workload runs in parallel'):
            deployed_nodes = preparation_result['deployed_nodes']
            execution_plan = preparation_result['execution_plan']
            overall_result = preparation_result['overall_result']
            
            if not deployed_nodes:
                logging.error("No deployed nodes available for execution")
                return {
                    'overall_result': overall_result,
                    'successful_runs': 0,
                    'total_runs': 0,
                    'total_execution_time': 0,
                    'workload_start_time': time(),
                    'deployed_nodes': []
                }
            
            # Параллельное выполнение на всех нодах
            with allure.step(f'Execute workload in parallel on {len(deployed_nodes)} nodes'):
                workload_start_time = time()
                
                # Запускаем nemesis через 15 секунд после начала выполнения workload
                if nemesis:
                    nemesis_thread = threading.Thread(
                        target=self._delayed_nemesis_start,
                        args=(15,),  # 15 секунд задержки
                        daemon=True
                    )
                    nemesis_thread.start()
                    logging.info("Scheduled nemesis to start in 15 seconds")
                
                # Для каждой ноды создаем свой план выполнения
                node_execution_plans = []
                for node in deployed_nodes:
                    # Каждая нода получает копию плана выполнения
                    node_plan = {
                        'node': node,
                        'plan': execution_plan
                    }
                    node_execution_plans.append(node_plan)
                
                logging.info(f"Starting parallel execution on {len(node_execution_plans)} nodes")
                
                # Результаты выполнения для каждой ноды
                node_results = []
                
                # Блокировка для безопасного обновления общих данных из потоков
                results_lock = threading.Lock()
                
                # Функция для выполнения workload на одной ноде
                def execute_on_node(node_plan):
                    node = node_plan['node']
                    plan = node_plan['plan']
                    node_host = node['node'].host
                    deployed_binary_path = node['binary_path']
                    
                    node_result = {
                        'node': node,
                        'host': node_host,
                        'successful_runs': 0,
                        'total_runs': len(plan),
                        'runs': [],
                        'total_execution_time': 0
                    }
                    
                    logging.info(f"Starting execution on node {node_host}")
                    
                    # Выполняем план для этой ноды
                    for run_num, run_config in enumerate(plan, 1):
                        # Формируем имя запуска с учетом ноды и chunk_num
                        chunk_num = run_config.get('chunk_num', run_num)
                        run_name = f"{workload_name}_{node_host}_chunk_{chunk_num}"
                        
                        # Добавляем информацию о ноде в run_config для статистики
                        run_config_copy = run_config.copy()
                        run_config_copy['node_host'] = node_host
                        run_config_copy['node_role'] = node['node'].role

                        # Выполняем один run
                        success, execution_time, stdout, stderr, is_timeout = self._execute_single_workload_run(
                            deployed_binary_path, node['node'], run_name,
                            command_args_template, duration_param, run_config_copy
                        )
                        
                        # Сохраняем результат запуска
                        run_result = {
                            'run_num': run_num,
                            'run_config': run_config_copy,
                            'success': success,
                            'execution_time': execution_time,
                            'stdout': stdout,
                            'stderr': stderr,
                            'is_timeout': is_timeout
                        }
                        node_result['runs'].append(run_result)
                        
                        # Обновляем статистику ноды
                        node_result['total_execution_time'] += execution_time
                        if success:
                            node_result['successful_runs'] += 1
                            logging.info(f"Run {run_num} on {node_host} completed successfully")
                        else:
                            logging.warning(f"Run {run_num} on {node_host} failed")
                        if not use_chunks:
                            break  # Прерываем при ошибке для одиночного запуска
                    
                    logging.info(f"Execution on {node_host} completed: "
                               f"{node_result['successful_runs']}/{node_result['total_runs']} successful")
                    
                    return node_result
                
                # Запускаем параллельное выполнение на всех нодах
                with ThreadPoolExecutor(max_workers=len(node_execution_plans)) as executor:
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
                            node_host = node_plan['node']['node'].host
                            logging.error(f"Error executing on {node_host}: {e}")
                            # Добавляем информацию об ошибке
                            node_results.append({
                                'node': node_plan['node'],
                                'host': node_host,
                                'error': str(e),
                                'successful_runs': 0,
                                'total_runs': len(node_plan['plan']),
                                'runs': [],
                                'total_execution_time': 0
                            })
                
                # Обрабатываем результаты всех нод
                successful_runs = 0
                total_runs = 0
                total_execution_time = 0
                
                # Обрабатываем результаты каждой ноды и добавляем их в общий результат
                for node_idx, node_result in enumerate(node_results):
                    node_host = node_result['host']
                    
                    # Добавляем статистику ноды в общую статистику
                    successful_runs += node_result['successful_runs']
                    total_runs += node_result['total_runs']
                    total_execution_time += node_result['total_execution_time']
                    
                    # Обрабатываем результаты каждого запуска на этой ноде
                    for run_result in node_result.get('runs', []):
                        # Генерируем уникальный номер запуска для всех нод
                        global_run_num = node_idx * len(execution_plan) + run_result['run_num']
                        
                        # Обрабатываем результат запуска
                        self._process_single_run_result(
                            overall_result, 
                            workload_name, 
                            global_run_num,
                            run_result['run_config'],
                            run_result['success'],
                            run_result['execution_time'],
                            run_result['stdout'],
                            run_result['stderr'],
                            run_result['is_timeout']
                        )
                
                logging.info(f"Parallel execution completed: {successful_runs}/{total_runs} successful runs "
                           f"on {len(node_results)} nodes")

                return {
                    'overall_result': overall_result,
                    'successful_runs': successful_runs,
                    'total_runs': total_runs,
                    'total_execution_time': total_execution_time,
                    'workload_start_time': workload_start_time,
                    'deployed_nodes': deployed_nodes,
                    'node_results': node_results
                }
                
    def _delayed_nemesis_start(self, delay_seconds: int):
        """
        Запускает nemesis с задержкой после начала выполнения workload
        
        Args:
            delay_seconds: Задержка в секундах перед запуском nemesis
        """
        try:
            logging.info(f"Nemesis will start in {delay_seconds} seconds...")
            time_module.sleep(delay_seconds)
            logging.info("Starting nemesis after delay")
            
            with allure.step('Start nemesis after delay'):
                self._manage_nemesis(enable_nemesis=True)
                
            logging.info("Nemesis started successfully after delay")
        except Exception as e:
            logging.error(f"Error starting nemesis after delay: {e}")
            allure.attach(str(e), 'Nemesis Error', attachment_type=allure.attachment_type.TEXT)

    def _deploy_workload_binary(self, workload_name: str, nodes_percentage: int = 100):
        """
        Выполняет deploy workload binary на указанный процент нод кластера
        
        Args:
            workload_name: Имя workload для отчетов
            nodes_percentage: Процент нод кластера для деплоя (от 1 до 100)
            
        Returns:
            Список словарей с информацией о нодах, на которые выполнен деплой:
            [{'node': node_object, 'binary_path': path_to_binary}, ...]
        """
        with allure.step('Deploy workload binary'):
            logging.info(f"Starting deployment for {workload_name} on {nodes_percentage}% of nodes")

            # Получаем бинарный файл
            with allure.step('Get workload binary'):
                binary_files = [yatest.common.binary_path(os.getenv(self.workload_env_var, 'ydb/tests/stress/' + self.workload_binary_name + '/' + self.workload_binary_name))]
                allure.attach(f"Environment variable: {self.workload_env_var}", 'Binary Configuration', attachment_type=allure.attachment_type.TEXT)
                allure.attach(f"Binary path: {binary_files[0]}", 'Binary Path', attachment_type=allure.attachment_type.TEXT)
                logging.info(f"Binary path resolved: {binary_files[0]}")

            # Получаем уникальные хосты кластера
            with allure.step('Select unique cluster hosts'):
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
                num_nodes = max(1, int(len(unique_nodes) * nodes_percentage / 100))
                # Выбираем первые N нод из списка уникальных
                selected_nodes = unique_nodes[:num_nodes]
                
                allure.attach(f"Selected {len(selected_nodes)}/{len(unique_nodes)} unique hosts ({nodes_percentage}%)", 
                             'Target Hosts', attachment_type=allure.attachment_type.TEXT)
                logging.info(f"Selected {len(selected_nodes)}/{len(unique_nodes)} unique hosts for deployment")

            # Развертываем бинарный файл на выбранных нодах
            with allure.step(f'Deploy {self.workload_binary_name} to {len(selected_nodes)} hosts'):
                target_hosts = [node.host for node in selected_nodes]
                logging.info(f"Starting deployment to hosts: {target_hosts}")
                
                deploy_results = deploy_binaries_to_hosts(
                    binary_files, target_hosts, self.binaries_deploy_path
                )
                
                # Собираем информацию о результатах деплоя
                deployed_nodes = []
                failed_nodes = []
                
                for node in selected_nodes:
                    binary_result = deploy_results.get(node.host, {}).get(self.workload_binary_name, {})
                    success = binary_result.get('success', False)
                    
                    if success:
                        deployed_nodes.append({
                            'node': node,
                            'binary_path': binary_result['path']
                        })
                        logging.info(f"Deployment successful on {node.host}: {binary_result['path']}")
                    else:
                        failed_nodes.append({
                            'node': node,
                            'error': binary_result.get('error', 'Unknown error')
                        })
                        logging.error(f"Deployment failed on {node.host}: {binary_result.get('error', 'Unknown error')}")
                
                # Прикрепляем детали развертывания
                allure.attach(
                    f"Successful deployments: {len(deployed_nodes)}/{len(selected_nodes)}\n" +
                    f"Failed deployments: {len(failed_nodes)}/{len(selected_nodes)}",
                    'Deployment Summary',
                    attachment_type=allure.attachment_type.TEXT
                )
                
                # Проверяем, что хотя бы одна нода успешно развернута
                if not deployed_nodes:
                    # Создаем детальное сообщение об ошибке деплоя
                    deploy_error_details = []
                    deploy_error_details.append(f"DEPLOYMENT FAILED: {self.workload_binary_name}")
                    deploy_error_details.append(f"Target hosts: {target_hosts}")
                    deploy_error_details.append(f"Target directory: {self.binaries_deploy_path}")
                    deploy_error_details.append(f"Binary environment variable: {self.workload_env_var}")
                    deploy_error_details.append(f"Local binary path: {binary_files[0]}")
                    
                    # Детали ошибок
                    deploy_error_details.append("\nDeployment errors:")
                    for failed in failed_nodes:
                        deploy_error_details.append(f"  {failed['node'].host}: {failed['error']}")
                    
                    detailed_deploy_error = "\n".join(deploy_error_details)
                    logging.error(detailed_deploy_error)
                    raise Exception(detailed_deploy_error)
                
                logging.info(f"Binary deployed successfully to {len(deployed_nodes)} unique hosts")
                return deployed_nodes

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
        return [{'duration': total_duration, 'run_type': 'single', 'chunk_num': 1}]

    def _execute_single_workload_run(self, deployed_binary_path: str, target_node, run_name: str, command_args_template: str, duration_param: str, run_config: dict):
        """Выполняет один запуск workload"""

        # Формируем команду
        if duration_param is None:
            # Используем command_args_template как есть (для обратной совместимости)
            command_args = command_args_template
        else:
            # Добавляем duration параметр
            command_args = f"{command_args_template} {duration_param} {run_config['duration']}"

        # Добавляем информацию о chunk_num в имя запуска, если она доступна
        if 'chunk_num' in run_config:
            run_name = f"{run_name}_chunk_{run_config['chunk_num']}"

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
                "chunk_num": run_config.get('chunk_num', 1),  # Добавляем chunk_num в статистику
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
            self.process_workload_result_with_diagnostics(overall_result, workload_name, False, use_node_subcols=True)

            logging.info(f"Final result: success={overall_result.success}, successful_runs={successful_runs}/{total_runs}")
            return overall_result
