import logging
from typing import Optional

from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class RunConfigInfo:
    test_start_time: int = None
    nodes_percentage: float = None
    nemesis_enabled: bool = None
    table_type: str = None
    stres_util_args: str = None
    duration: float = None


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


class StressUtilNodeResult:
    """Aggregates results of multiple runs for a single stress test

    Attributes:
        stress_name: Name of stress util
        node: Node identifier where test ran
        host: Hostname where test ran
        start_time: Timestamp when test started
        end_time: Timestamp when test ended
        total_execution_time: Total duration across all runs
        successful_runs: Count of successful runs
        total_runs: Total count of runs
        runs: List of individual run results
    """
    stress_name: str = None
    node: str = None
    host: str = None
    start_time: float = None
    end_time: float = None
    total_execution_time: str = None
    runs: list[StressUtilRunResult] = []

    def __init__(self):
        """Initialize StressUtilNodeResult with empty runs list"""
        self.runs = []

    def __repr__(self):
        return (f"StressUtilNodeResult(stress_name={self.stress_name}, "
                f"node={self.node}, host={self.host}, "
                f"start_time={self.start_time}, "
                f"end_time={self.end_time}, "
                f"total_execution_time={self.total_execution_time}, "
                f"runs={self.runs})")

    def get_successful_runs(self) -> int:
        """Get count of successful runs

        Returns:
            int: Total count of successful runs
        """
        return sum(run_result.is_success for run_result in self.runs)

    def get_total_runs(self) -> int:
        """Get total count of runs

        Returns:
            int: Total count of runs
        """
        return len(self.runs)

    def is_all_success(self) -> bool:
        """Check if all runs across all nodes were successful

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.get_successful_runs() == self.get_total_runs()


class StressUtilResult:
    """Aggregates results of multiple runs for a single stress test

    Attributes:
        start_time: Timestamp when test started
        end_time: Timestamp when test ended
        total_execution_time: Total duration across all runs
        runs: List of individual run results
    """
    start_time: float = None
    end_time: float = None
    execution_args: list[str] = None
    node_runs: dict[str, StressUtilNodeResult] = dict()

    def __init__(self):
        """Initialize StressUtilResult with empty runs list"""
        self.node_runs = dict()
        self.execution_args = []

    def __repr__(self):
        return f"StressUtilResult(start_time={self.start_time}, end_time={self.end_time}, node_runs={self.node_runs})"

    def get_successful_runs(self) -> int:
        """Get count of successful runs

        Returns:
            int: Total count of successful runs
        """
        return sum(run_result.get_successful_runs() for run_result in self.node_runs.values())

    def get_total_runs(self) -> int:
        """Get total count of runs

        Returns:
            int: Total count of runs
        """
        return sum(run_result.get_total_runs() for run_result in self.node_runs.values())

    def is_all_success(self) -> bool:
        """Check if all runs across all nodes were successful

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.get_total_runs() == self.get_successful_runs()


class StressUtilDeployResult:
    """Stores deployment results for a stress test

    Attributes:
        stress_name: Name of the stress test
        nodes: List of nodes where test was deployed
        hosts: List of hostnames where test was deployed
    """
    stress_name: str = None
    nodes: list[YdbCluster.Node] = None
    hosts: list[str] = None

    def __init__(self):
        """Initialize StressUtilDeployResult with empty nodes/hosts"""
        self.nodes = None
        self.hosts = None


class StressUtilTestResults:
    """Aggregates results from all stress test runs

    Attributes:
        start_time: Timestamp when testing started
        end_time: Timestamp when testing ended
        stress_util_runs: Dictionary mapping test names to their results
        nemesis_deploy_results: Dictionary of nemesis deployment results
        errors: List of error messages
        error_message: Combined error message string
    """
    start_time: float = None
    end_time: float = None
    stress_util_runs: dict[str, StressUtilResult] = dict()
    nemesis_deploy_results: dict[str, list[str]] = dict()
    errors: list[str] = []
    error_message: str = ''

    def __init__(self):
        """Initialize StressUtilTestResults with empty collections"""
        self.start_time = None
        self.end_time = None
        self.stress_util_runs = dict()
        self.nemesis_deploy_results = dict()
        self.errors = []
        self.error_message = ''

    def add_error(self, msg: Optional[str]) -> bool:
        """Add an error message to the test results

        Args:
            msg: Error message to add (can be None)

        Returns:
            bool: True if message was added, False if msg was None
        """
        if msg:
            self.errors.append(msg)
            if len(self.error_message) > 0:
                self.error_message += f'\n\n{msg}'
            else:
                self.error_message = msg
            return True
        return False

    def get_stress_successful_runs(self, stress_name: str) -> int:
        """Get count of successful runs for a specific stress test

        Args:
            stress_name: Name of the stress test to query

        Returns:
            int: Total count of successful runs
        """
        return self.stress_util_runs[stress_name].get_successful_runs()

    def get_stress_total_runs(self, stress_name: str) -> int:
        """Get total count of runs for a specific stress test

        Args:
            stress_name: Name of the stress test to query

        Returns:
            int: Total count of runs
        """
        return self.stress_util_runs[stress_name].get_total_runs()

    def is_success_for_a_stress(self, stress_name: str) -> bool:
        """Check if all runs in a stress test were successful

        Args:
            stress_name: Name of the stress test to query

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.stress_util_runs[stress_name].is_all_success()

    def is_all_success(self) -> bool:
        """Check if all runs across all stress tests were successful

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return all(result.is_all_success() for result in self.stress_util_runs.values())

    def get_successful_runs(self) -> int:
        """Get count of successful runs

        Returns:
            int: Total count of successful runs
        """
        return sum(stress_info.get_successful_runs() for stress_info in self.stress_util_runs.values())

    def get_total_runs(self) -> int:
        """Get total count of runs

        Returns:
            int: Total count of runs
        """
        return sum(stress_info.get_total_runs() for stress_info in self.stress_util_runs.values())

    def __repr__(self):
        return (
            f"StressUtilTestResults(start_time={self.start_time}, "
            f"end_time={self.end_time}, "
            f"stress_util_runs={self.stress_util_runs}, "
            f"nemesis_deploy_results={self.nemesis_deploy_results}, "
            f"errors={self.errors})"
        )


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
    Creates and populates a WorkloadRunResult with common logic

    Args:
        workload_name: Workload name for statistics
        stdout: Workload stdout output
        stderr: Workload stderr output
        success: Whether execution was successful
        additional_stats: Additional statistics
        is_timeout: Timeout flag
        iteration_number: Iteration number
        actual_execution_time: Actual execution time

    Returns:
        Populated WorkloadRunResult object
    """
    result = YdbCliHelper.WorkloadRunResult()
    result.start_time = start_time
    result.stdout = str(stdout)
    result.stderr = str(stderr)

    # Add diagnostic information about input data
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

    # Analyze execution results
    error_found = False

    # Check for timeout first (this is a warning, not an error)
    if is_timeout:
        result.add_warning(
            f"Workload execution timed out. stdout: {stdout}, stderr: {stderr}"
        )
    else:
        # Check for explicit errors (only if not timeout)
        if not success:
            result.add_error(f"Workload execution failed. stderr: {stderr}")
            error_found = True
        elif not ignore_stderr_content:
            if "error" in str(stderr).lower():
                result.add_error(f"Error detected in stderr: {stderr}")
                error_found = True
            elif _has_real_error_in_stdout(str(stdout)):
                result.add_warning(f"Error detected in stdout: {stdout}")
                error_found = True

    # Check for warnings
    if (
        "warning: permanently added" not in str(stderr).lower()
        and "warning" in str(stderr).lower()
    ):
        result.add_warning(f"Warning in stderr: {stderr}")

    # Add execution information to iterations
    iteration = YdbCliHelper.Iteration()
    # Use actual execution time if specified, otherwise
    # use planned time
    execution_time = (
        actual_execution_time if actual_execution_time is not None else 0
    )
    iteration.time = execution_time

    # Add iteration name for better identification
    if additional_stats:
        node_host = additional_stats.get("node_host", "")
        iteration_num = additional_stats.get(
            "iteration_num", iteration_number)

        # Check if iter_ prefix was already added to the name
        iter_prefix_added = additional_stats.get(
            "iter_prefix_added", False)

        # Form the iteration name
        if iter_prefix_added:
            # If prefix was already added, just use workload name
            iteration.name = f"{workload_name}_{node_host}"
        else:
            # If prefix wasn't added yet, add it
            iteration.name = f"{workload_name}_{node_host}_iter_{iteration_num}"

        # Add iteration and thread statistics to the iteration
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

    # Add basic statistics
    result.add_stat(
        workload_name, "execution_time", execution_time
    )  # Use actual execution time
    result.add_stat(
        workload_name, "planned_duration", 0
    )  # Add planned duration separately
    # Timeout is not considered a failed execution, it's a warning
    result.add_stat(
        workload_name,
        "workload_success",
        (success and not error_found) or is_timeout,
    )
    result.add_stat(
        workload_name,
        "success_flag",
        success)  # Исходный флаг успеха

    # Add additional statistics if present
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
    Checks if there are real errors in stdout, excluding workload statistics

    Args:
        stdout: stdout output to analyze

    Returns:
        bool: True if real errors are found
    """
    if not stdout:
        return False

    stdout_lower = stdout.lower()

    # List of patterns that are NOT errors (workload statistics)
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

    # Check if "error" appears in a context that is NOT
    # a false positive
    error_positions = []
    start_pos = 0
    while True:
        pos = stdout_lower.find("error", start_pos)
        if pos == -1:
            break
        error_positions.append(pos)
        start_pos = pos + 1

    # For each found "error", check its context
    for pos in error_positions:
        # Get context around found "error" (50 chars before and after)
        context_start = max(0, pos - 50)
        context_end = min(len(stdout), pos + 50)
        context = stdout[context_start:context_end].lower()

        # Check if this is a false positive
        is_false_positive = any(
            pattern in context for pattern in false_positive_patterns
        )

        if not is_false_positive:
            # Additional checks for real error messages
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

            # If real error indicator found, return True
            if any(indicator in context for indicator in real_error_indicators):
                return True

    return False


def analyze_execution_results(
    overall_result,
    successful_runs: int,
    total_runs: int,
    use_iterations: bool,
):
    """Analyze execution results and add appropriate errors/warnings

    Args:
        overall_result: Result object to modify
        successful_runs: Number of successful runs
        total_runs: Total number of runs
        use_iterations: Whether iterations were used in the test
    """
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

        # Check statistics
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
    """Collect and add execution statistics to the result object

    Args:
        overall_result: Result object to modify
        workload_name: Name of the workload
        execution_result: Dictionary of execution results
        additional_stats: Additional statistics to include
        duration_value: Planned execution duration
        use_iterations: Whether iterations were used in the test
    """
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
