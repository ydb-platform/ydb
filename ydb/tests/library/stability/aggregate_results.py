import logging
from typing import Optional

from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class StressUtilRunResult:
    run_config: dict = None
    is_success: bool = None
    is_timeout: bool = None
    iteration_number: int = None
    stdout: str = None
    stderr: str = None
    start_time: float = None
    end_time: float = None
    execution_time: str = None


class StressUtilResult:
    stress_name: str = None
    node: str = None
    host: str = None
    resolution: str = None
    start_time: float = None
    end_time: float = None
    total_execution_time: str = None
    successful_runs: int = None
    total_runs: int = None
    runs: list[StressUtilRunResult] = []

    def __init__(self):
        self.runs = []


class StressUtilDeployResult:
    stress_name: str = None
    nodes: list[YdbCluster.Node] = None
    hosts: list[str] = None

    def __init__(self):
        self.nodes = None
        self.hosts = None


class StressUtilTestResults:
    start_time: float = None
    end_time: float = None
    stress_util_runs: dict[str, list[StressUtilResult]] = dict()
    nemesis_deploy_results: dict[str, list[str]] = dict()
    errors: list[str] = []
    error_message: str = ''

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.stress_util_runs = dict()
        self.nemesis_deploy_results = dict()
        self.errors = []
        self.error_message = ''

    def add_error(self, msg: Optional[str]) -> bool:
        if msg:
            self.errors.append(msg)
            if len(self.error_message) > 0:
                self.error_message += f'\n\n{msg}'
            else:
                self.error_message = msg
            return True
        return False

    def get_stress_successful_runs(self, stress_name: str) -> int:
        return sum(run_result.successful_runs for run_result in self.stress_util_runs[stress_name])

    def get_stress_total_runs(self, stress_name: str) -> int:
        return sum(run_result.total_runs for run_result in self.stress_util_runs[stress_name])

    def is_all_success(self) -> bool:
        return all(all(run_result.successful_runs == run_result.total_runs for run_result in result) for result in self.stress_util_runs.values())

    def __repr__(self):
        return f"StressUtilTestResults(start_time={self.start_time}, end_time={self.end_time}, stress_util_runs={self.stress_util_runs}, nemesis_deploy_results={self.nemesis_deploy_results}, errors={self.errors})"


def process_single_run_result(
    workload_name: str,
    run_num: int,
    run_config: dict,
    success: bool,
    execution_time: float,
    start_time: float,
    stdout: str,
    stderr: str,
    is_timeout: bool,
    ignore_stderr_content: bool
) -> YdbCliHelper.WorkloadRunResult:
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
    run_result = create_workload_result(
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
        start_time=start_time,
        ignore_stderr_content=ignore_stderr_content,
    )
    return run_result


def create_workload_result(
    workload_name: str,
    stdout: str,
    stderr: str,
    success: bool,
    additional_stats: dict = None,
    is_timeout: bool = False,
    iteration_number: int = 0,
    actual_execution_time: float = None,
    start_time: float = None,
    ignore_stderr_content: bool = False,
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
    result.start_time = start_time
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
        elif not ignore_stderr_content:
            if "error" in str(stderr).lower():
                result.add_error(f"Error detected in stderr: {stderr}")
                error_found = True
            elif _has_real_error_in_stdout(str(stdout)):
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
        actual_execution_time if actual_execution_time is not None else 0
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
        workload_name, "planned_duration", 0
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


def _has_real_error_in_stdout(stdout: str) -> bool:
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


def analyze_execution_results(
    overall_result,
    successful_runs: int,
    total_runs: int,
    use_iterations: bool,
):
    """
    Analyzes execution results and adds errors/warnings

    Args:
        overall_result: Overall result to add info to
        successful_runs: Number of successful runs
        total_runs: Total number of runs
        use_iterations: Whether iterations were used
    """
    # Group iterations by number to determine actual
    # iteration count
    iterations_by_number = {}
    threads_by_iteration = {}

    # Analyze all iterations and group them by iteration number
    for iter_num, iteration in overall_result.iterations.items():
        # Get iteration number from stats or name
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

        # If not found in stats, try to extract from name
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

        # If still not determined, use iteration number
        if real_iter_num is None:
            real_iter_num = iter_num

        # Add iteration to its number group
        if real_iter_num not in iterations_by_number:
            iterations_by_number[real_iter_num] = []
            threads_by_iteration[real_iter_num] = set()

        iterations_by_number[real_iter_num].append(iteration)
        if node_host:
            threads_by_iteration[real_iter_num].add(node_host)

    # Determine actual iteration and thread counts
    real_iteration_count = len(iterations_by_number)
    failed_iterations = 0

    # Check each iteration for errors
    for iter_num, iterations in iterations_by_number.items():
        # If at least one thread in iteration succeeded, consider
        # iteration successful
        iteration_success = any(
            not hasattr(
                iter_obj, "error_message") or not iter_obj.error_message
            for iter_obj in iterations
        )

        if not iteration_success:
            failed_iterations += 1

    # Create error or warning message
    if failed_iterations == real_iteration_count and real_iteration_count > 0:
        # All iterations failed
        threads_info = ""
        if (
            real_iteration_count == 1
            and sum(len(threads) for threads in threads_by_iteration.values()) > 1
        ):
            # If there was only one iteration with multiple threads,
            # indicate this
            thread_count = sum(
                len(threads) for threads in threads_by_iteration.values()
            )
            threads_info = f" with {thread_count} parallel threads"

        overall_result.add_error(
            f"All {real_iteration_count} iterations{threads_info} failed to execute successfully"
        )
    elif failed_iterations > 0:
        # Some iterations failed
        overall_result.add_warning(
            f"{failed_iterations} out of {real_iteration_count} iterations failed to execute successfully"
        )


def add_execution_statistics(
    overall_result,
    workload_name: str,
    execution_result: dict,
    additional_stats: dict,
    duration_value: float,
    use_iterations: bool,
):
    """
    Collects and adds execution statistics

    Args:
        overall_result: Overall result to add stats to
        workload_name: Workload name
        execution_result: Execution results
        additional_stats: Additional statistics
        duration_value: Execution time in seconds
        use_iterations: Whether iterations were used
    """
    successful_runs = execution_result["successful_runs"]
    total_runs = execution_result["total_runs"]
    total_execution_time = execution_result["total_execution_time"]

    # Group iterations by number to determine actual
    # iteration and thread counts
    iterations_by_number = {}
    threads_by_iteration = {}

    # Analyze all iterations and group them by iteration number
    for iter_num, iteration in overall_result.iterations.items():
        # Get iteration number from stats or name
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

        # If not found in stats, try to extract from name
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

        # If still not determined, use iteration number
        if real_iter_num is None:
            real_iter_num = iter_num

        # Add iteration to its number group
        if real_iter_num not in iterations_by_number:
            iterations_by_number[real_iter_num] = []
            threads_by_iteration[real_iter_num] = set()

        iterations_by_number[real_iter_num].append(iteration)
        if node_host:
            threads_by_iteration[real_iter_num].add(node_host)

    # Determine actual iteration and thread counts
    real_iteration_count = len(iterations_by_number)
    total_thread_count = sum(
        len(threads) for threads in threads_by_iteration.values()
    )

    # Count successful iterations
    successful_iterations = 0
    for iter_num, iterations in iterations_by_number.items():
        # If at least one thread in iteration succeeded, consider
        # iteration successful
        iteration_success = any(
            not hasattr(
                iter_obj, "error_message") or not iter_obj.error_message
            for iter_obj in iterations
        )

        if iteration_success:
            successful_iterations += 1

    # Calculate actual execution time as average across iterations
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

    # Calculate average actual execution time
    avg_actual_time = (
        sum(actual_execution_times) / len(actual_execution_times)
        if actual_execution_times
        else duration_value
    )

    # Basic statistics
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

    # Add additional statistics
    if additional_stats:
        stats.update(additional_stats)

    # Add all statistics to result
    for key, value in stats.items():
        overall_result.add_stat(workload_name, key, value)
