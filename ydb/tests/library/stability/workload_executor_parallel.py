import allure
import logging
import time as time_module
import pytest

from ydb.tests.library.stability.aggregate_results import StressUtilDeployResult, StressUtilTestResults
from ydb.tests.library.stability.build_report import create_parallel_allure_report
from ydb.tests.library.stability.upload_results import RunConfigInfo, safe_upload_results
from ydb.tests.olap.lib.utils import external_param_is_true, get_external_param
from ydb.tests.library.stability.deploy import StressUtilDeployer
from ydb.tests.library.stability.run_stress import StressRunExecutor
from ydb.tests.olap.load.lib.conftest import LoadSuiteBase


class ParallelWorkloadTestBase:
    """
    Base class for workload tests with common functionality
    """

    # Attributes that can be overridden in child classes
    binaries_deploy_path: str = (
        "/tmp/stress_binaries/"  # Path for deploying binary files
    )
    timeout: float = 1800.0  # Default timeout
    _nemesis_started: bool = False  # Flag to track nemesis startup
    cluster_path: str = str(
        get_external_param(
            "cluster_path",
            ""))  # Path to cluster
    yaml_config: str = str(
        get_external_param(
            "yaml-config",
            ""))  # Path to yaml configuration

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
        Common initialization for workload tests.
        Do NOT perform _Verification here - we'll do it before each test.
        """
        with allure.step("Workload test setup: initialize"):
            self._setup_start_time = time_module.time()
            self._ignore_stderr_content = external_param_is_true('ignore_stderr_content')

    def execute_parallel_workloads_test(
        self,
        stress_executor: StressRunExecutor,
        stress_deployer: StressUtilDeployer,
        olap_load_base: LoadSuiteBase,
        workload_params: dict,
        duration_value: float = None,
        nemesis_enabled: bool = False,
        nodes_percentage: int = 100,
    ):
        """
        Executes full workload test cycle with three phases:
        1. Preparation (deploy binaries to nodes)
        2. Execution (run workloads in parallel)
        3. Results collection and diagnostics

        Args:
            stress_executor: Stress test executor instance
            stress_deployer: Stress test deployer instance
            olap_load_base: Load test base configuration
            workload_params: Dictionary of workload configurations
            duration_value: Execution time in seconds (None uses self.timeout)
            nemesis_enabled: Whether to start nemesis after 15 seconds
            nodes_percentage: Percentage of nodes to use (1-100)

        Returns:
            None (results are processed internally and reported via Allure)

        Raises:
            ValueError: If nodes_percentage is invalid
        """

        if duration_value is None:
            duration_value = self.timeout

        # Validate nodes percentage
        if nodes_percentage < 1 or nodes_percentage > 100:
            raise ValueError(
                f"nodes_percentage must be between 1 and 100, got: {nodes_percentage}"
            )

        additional_stats = RunConfigInfo()
        additional_stats.nemesis_enabled = nemesis_enabled
        additional_stats.nodes_percentage = nodes_percentage
        additional_stats.test_start_time = int(time_module.time())
        additional_stats.duration = duration_value

        logging.info("=== Starting environment preparation ===")

        # PHASE 1: PREPARATION (deploy binaries to nodes)
        preparation_result = stress_deployer.prepare_stress_execution(olap_load_base, workload_params, nodes_percentage)

        logging.debug(f"Deploy finished with {preparation_result}")

        # PHASE 2: EXECUTION (run workloads in parallel)
        execution_result = stress_executor.execute_stress_runs(
            stress_deployer,
            workload_params,
            duration_value,
            preparation_result,
            nemesis_enabled,
        )
        logging.debug(f"Execution finished with {execution_result}")
        logging.debug(f"Additional stats {additional_stats}")

        # PHASE 3: RESULTS (collect diagnostics and finalize)
        self._finalize_workload_results(
            olap_load_base,
            execution_result,
            preparation_result['deployed_nodes'],
            additional_stats
        )

        logging.info("=== Workload test completed ===")
        # logging.debug(f"Execution final result {final_result}")

    def _finalize_workload_results(
        self,
        olap_load_base: LoadSuiteBase,
        execution_result: dict,
        preparation_result: dict[str, StressUtilDeployResult],
        run_config: RunConfigInfo
    ):
        """
        PHASE 3: Finalizing results and diagnostics

        Args:
            execution_result: Execution results
            additional_stats: Additional statistics
            duration_value: Execution time in seconds
            use_chunks: Whether to use iteration splitting (deprecated parameter)

        Returns:
            Final execution result
        """

        with allure.step("Phase 3: Finalize results and diagnostics"):
            overall_result = execution_result["overall_result"]
            successful_runs = execution_result["successful_runs"]
            total_runs = execution_result["total_runs"]

            # Final processing with diagnostics (prepares data for upload)
            overall_result.workload_start_time = execution_result["workload_start_time"]
            node_errors, verify_errors = self.process_workload_result_with_diagnostics(olap_load_base, overall_result)

            # Separate step for uploading results (AFTER all data preparation)
            safe_upload_results(overall_result, run_config, node_errors, verify_errors)

            # Final status processing (may throw exception, but results are already uploaded)
            # Use node_errors saved from diagnostics
            self._handle_final_status(overall_result, preparation_result, node_errors, verify_errors)

            logging.info(
                f"Final result: successful_runs={successful_runs} / {total_runs}"
            )
            return overall_result

    def process_workload_result_with_diagnostics(
        self,
        olap_load_base: LoadSuiteBase,
        result: StressUtilTestResults,
    ):
        """
        Processes workload result with diagnostic information

        Args:
            olap_load_base: Load test base configuration
            result: Workload execution results

        Returns:
            Tuple containing:
            - node_errors: List of node error objects
            - verify_errors: Dictionary of verification errors

        Note:
            - Collects node diagnostics and verification results
            - Generates Allure report with detailed information
            - Handles error cases gracefully
        """
        with allure.step("Process workload result"):
            node_errors = []
            verify_errors = {}

            # Check node status and collect errors
            try:
                # Use parent class method for node diagnostics
                end_time = time_module.time()
                diagnostics_start_time = result.start_time
                verify_errors = olap_load_base.check_nodes_verifies_with_timing(diagnostics_start_time, end_time)
                node_errors = olap_load_base.check_nodes_diagnostics_with_timing(
                    result, diagnostics_start_time, end_time
                )
            except Exception as e:
                logging.error(f"Error getting nodes state: {e}")
                # Добавляем ошибку в результат
                result.add_warning(f"Error getting nodes state: {e}")
                node_errors = []  # Set empty list if diagnostics failed

            # Вычисляем время выполнения
            end_time = time_module.time()

            # --- IMPORTANT: set nodes_with_issues for proper fail ---

            # Prepare error lists for upload
            node_error_messages = []
            workload_error_messages = []

            # Collect node errors with details
            for node_error in node_errors:
                if node_error.core_hashes:
                    for core_id, core_hash in node_error.core_hashes:
                        node_error_messages.append(f"Node {node_error.node.slot} coredump {core_id}")
                if node_error.was_oom:
                    node_error_messages.append(f"Node {node_error.node.slot} experienced OOM")
                verify_fails_count = 0
                for verify_summary, detailed_verify_errors in verify_errors.items():
                    if node_error.node.host in detailed_verify_errors['hosts_count']:
                        verify_fails_count += detailed_verify_errors['hosts_count'][node_error.node.host]
                if verify_fails_count:
                    node_error_messages.append(f"Node {node_error.node.host} had {verify_fails_count} VERIFY fails")
                if hasattr(node_error, 'sanitizer_errors') and node_error.sanitizer_errors > 0:
                    node_error_messages.append(f"Node {node_error.node.host} has {node_error.sanitizer_errors} SAN errors")

            # Collect workload errors (not related to nodes)
            if result.errors:
                for err in result.errors:
                    if "coredump" not in err.lower() and "oom" not in err.lower():
                        workload_error_messages.append(err)

            # 4. Generate allure report
            create_parallel_allure_report(result, node_errors, verify_errors)

            # Save node_errors for use after upload
            # result._node_errors = node_errors

            # Data is ready, now we can upload results
            return node_errors, verify_errors

    def _handle_final_status(self, result: StressUtilTestResults, preparation_result: dict[str, StressUtilDeployResult], node_errors, verify_errors):
        """
        Handles final test status (fail, broken, etc.)

        Args:
            result: Test results object
            node_errors: List of node errors
            verify_errors: Verification errors

        Raises:
            pytest.fail: If nodes have coredumps/OOMs
            Exception: If workload errors occurred
        """
        nodes_with_issues = len(node_errors) + len(verify_errors)
        workload_errors = []
        if result.errors:
            for err in result.errors:
                if "coredump" not in err.lower() and "oom" not in err.lower():
                    workload_errors.append(err)

        # --- Переключатель: если cluster_log=all, то всегда прикладываем логи ---
        cluster_log_mode = get_external_param('cluster_log', 'default')
        try:
            if cluster_log_mode == 'all' or nodes_with_issues > 0 or workload_errors:
                LoadSuiteBase.__attach_logs(
                    start_time=result.start_time,
                    attach_name="kikimr",
                    query_text="",
                    ignore_roles=True  # Collect logs from all unique hosts
                )
        except Exception as e:
            logging.warning(f"Failed to attach kikimr logs: {e}")

        # --- FAIL TEST IF CORES OR OOM FOUND ---
        if nodes_with_issues > 0:
            error_msg = f"Test failed: found {nodes_with_issues} issue(s) with coredump(s), OOM(s), VERIFY fail(s) or SAN errors"
            pytest.fail(error_msg)
        # --- MARK TEST AS BROKEN IF WORKLOAD ERRORS (not cores/oom) ---
        if workload_errors:
            raise Exception("Test marked as broken due to workload errors: " + "; ".join(workload_errors))

        # В диагностическом режиме не падаем из-за предупреждений о coredump'ах/OOM
        if not result.is_all_success() and result.error_message:
            # Создаем детальное сообщение об ошибке с контекстом
            error_details = []
            error_details.append("TEST EXECUTION FAILED: ")
            error_details.append(f"Main error: {result.error_message}")
            if result.stress_util_runs:
                error_details.append("\nExecution details:")
                error_details.append(f"Total iterations attempted: {len(result.iterations)}")
                failed_iterations = []
                successful_iterations = []
                for workload_name, workload_run_info in result.stress_util_runs.items():
                    if workload_run_info.get_successful_runs == 0:
                        failed_iterations.append({
                            'iteration': workload_name,
                            'error': "All runs on all nodes have failed",
                        })
                    else:
                        successful_iterations.append({
                            'iteration': workload_name,
                        })
                if failed_iterations:
                    error_details.append(f"\nFAILED RUNS ({len(failed_iterations)}):")
                    for fail_info in failed_iterations:
                        error_details.append(f"  - Stress util {fail_info['iteration']}: {fail_info['error']}")
                if successful_iterations:
                    error_details.append(f"\nSuccessful runs: ({len(successful_iterations)}):")
                    for success_info in successful_iterations:
                        error_details.append(f"  - Iteration {success_info['iteration']}: OK")
            error_details.append("\nRUN STATISTICS:")
            error_details.append(f"  Successful runs: {result.get_successful_runs()}/{result.get_total_runs()}")
            error_details.append(f"  Successful runs: {result.get_successful_runs()}")
            error_details.append(f"  Failed runs: {result.get_total_runs() - result.get_successful_runs()}")
            error_details.append(f"  Success rate: {(result.get_successful_runs() / result.get_total_runs()):.1%}")
            error_details.append("\nDEPLOYMENT INFO:")
            for stress_name, deploy_info in preparation_result.items():
                error_details.append(f"  {stress_name}:")
                for host_info in deploy_info.hosts:
                    error_details.append(f"    {host_info}")
            detailed_error_message = "\n".join(error_details)
            exc = pytest.fail.Exception(detailed_error_message)
            raise exc
        # if result.warning_message:
        #     logging.warning(f"Workload completed with warnings: {result.warning_message}")
