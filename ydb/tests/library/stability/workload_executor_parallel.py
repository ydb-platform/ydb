from typing import Optional
import warnings
import allure
import logging
import time as time_module
import pytest

from ydb.tests.library.stability.healthcheck.healthcheck_reporter import HealthCheckReporter
from ydb.tests.library.stability.utils.results_models import StressUtilDeployResult, StressUtilTestResults
from ydb.tests.library.stability.build_report import create_parallel_allure_report
from ydb.tests.library.stability.utils.collect_errors import ErrorsCollector
from ydb.tests.library.stability.utils.upload_results import RunConfigInfo, safe_upload_results, test_event_report
from ydb.tests.library.stability.utils.utils import external_param_is_true, get_external_param
from ydb.tests.library.stability.deploy import StressUtilDeployer
from ydb.tests.library.stability.run_stress import StressRunExecutor


class ParallelWorkloadTestBase:
    """
    Base class for workload tests with common functionality
    """

    # Attributes that can be overridden in child classes
    binaries_deploy_path: str = (
        "/tmp/stress_binaries/"  # Path for deploying binary files
    )
    timeout: float = 1800.0  # Default timeout
    cluster_path: str = str(
        get_external_param(
            "cluster_path",
            ""))  # Path to cluster
    yaml_config: str = str(
        get_external_param(
            "yaml-config",
            ""))  # Path to yaml configuration
    event_process_mode: str = get_external_param('event_process_mode', None)  # one of: save, send, both
    ignore_stderr_content: str = external_param_is_true('ignore_stderr_content')

    @pytest.fixture(autouse=True, scope="session")
    def binary_deployer(self):
        binaries_deploy_path: str = (
            "/tmp/stress_binaries/"
        )
        deployer = StressUtilDeployer(binaries_deploy_path, cluster_path=self.cluster_path, yaml_config=self.yaml_config)
        yield deployer
        deployer._manage_nemesis(False, [], 'teardown')

    @pytest.fixture(autouse=True, scope="session")
    def health_checker_daemon(self, binary_deployer: StressUtilDeployer):
        if self.event_process_mode is not None:
            reporter = HealthCheckReporter(binary_deployer.hosts)
            reporter.start_healthchecks()
            yield reporter
            reporter.stop_healthchecks()
        else:
            yield None

    @pytest.fixture(autouse=True, scope="session")
    def stress_executor(self) -> StressRunExecutor:
        return StressRunExecutor(self.ignore_stderr_content, self.event_process_mode)

    def execute_parallel_workloads_test(
        self,
        stress_executor: StressRunExecutor,
        stress_deployer: StressUtilDeployer,
        workload_params: dict[str, dict],
        duration_value: float = None,
        nemesis_enabled: bool = False,
        nodes_percentage: Optional[int] = None,
    ) -> None:
        """
        Executes full workload test cycle with three phases:
        1. Preparation (deploy binaries to nodes)
        2. Execution (run workloads in parallel)
        3. Results collection and diagnostics

        Args:
            stress_executor: Stress test executor instance
            stress_deployer: Stress test deployer instance
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
        if nodes_percentage and (nodes_percentage < 1 or nodes_percentage > 100):
            raise ValueError(
                f"nodes_percentage must be between 1 and 100, got: {nodes_percentage}"
            )

        additional_stats = RunConfigInfo()
        additional_stats.nemesis_enabled = nemesis_enabled
        additional_stats.nodes_percentage = nodes_percentage
        additional_stats.test_start_time = int(time_module.time())
        additional_stats.duration = duration_value
        additional_stats.all_hosts = stress_deployer.hosts
        additional_stats.stress_util_names = list(workload_params.keys())
        errors_collector = ErrorsCollector(additional_stats.all_hosts, stress_deployer.nodes)

        # Publish TestInit record
        with allure.step("Initialize test"):
            test_event_report('TestInit', workload_names=additional_stats.stress_util_names, nemesis_enabled=nemesis_enabled)

        # THEN execute cluster health check
        with allure.step("Pre-workload cluster verification"):
            errors_collector.perform_verification_with_cluster_check(workload_names=additional_stats.stress_util_names, nemesis_enabled=nemesis_enabled)

        logging.info("=== Starting environment preparation ===")

        # PHASE 1: PREPARATION (deploy binaries to nodes)
        preparation_result = stress_deployer.prepare_stress_execution(workload_params, nodes_percentage)

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

        if stress_deployer.nemesis_started:
            recoverability_result = self.stop_nemesis_and_check_recoverability(
                stress_executor,
                stress_deployer,
                workload_params,
                preparation_result)
            execution_result['overall_result'].recoverability_result = recoverability_result['overall_result']

        # PHASE 3: RESULTS (collect diagnostics and finalize)
        self._finalize_workload_results(
            stress_deployer,
            errors_collector,
            execution_result,
            preparation_result['deployed_nodes'],
            additional_stats
        )

        logging.info("=== Workload test completed ===")
        # logging.debug(f"Execution final result {final_result}")

    def _finalize_workload_results(
        self,
        stress_deployer: StressUtilDeployer,
        errors_collector: ErrorsCollector,
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
            overall_result: StressUtilTestResults = execution_result["overall_result"]
            successful_runs = execution_result["successful_runs"]
            total_runs = execution_result["total_runs"]

            if overall_result.recoverability_result is None:
                errors_collector.check_nemesis_status(stress_deployer.nemesis_started, run_config.nemesis_enabled, run_config.stress_util_names)

            # Final processing with diagnostics (prepares data for upload)
            overall_result.workload_start_time = execution_result["workload_start_time"]
            node_errors, verify_errors = self.process_workload_result_with_diagnostics(errors_collector, overall_result)

            # Separate step for uploading results (AFTER all data preparation)
            safe_upload_results(overall_result, run_config, node_errors, verify_errors)

            # Final status processing (may throw exception, but results are already uploaded)
            # Use node_errors saved from diagnostics
            self._handle_final_status(errors_collector, overall_result, preparation_result, node_errors, verify_errors)

            logging.info(
                f"Final result: successful_runs={successful_runs} / {total_runs}"
            )
            return overall_result

    def process_workload_result_with_diagnostics(
        self,
        errors_collector: ErrorsCollector,
        result: StressUtilTestResults,
    ) -> tuple[list, dict]:
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
                verify_errors = errors_collector.check_nodes_verifies_with_timing(diagnostics_start_time, end_time)
                node_errors = errors_collector.check_nodes_diagnostics_with_timing(
                    result, diagnostics_start_time, end_time
                )
            except Exception as e:
                logging.error(f"Error getting nodes state: {e}")
                # Add error to result
                node_errors = []  # Set empty list if diagnostics failed

            # Calculate execution time
            end_time = time_module.time()

            # --- IMPORTANT: set nodes_with_issues for proper fail ---

            # Prepare error lists for upload
            node_error_messages = []
            workload_error_messages = []

            # Collect node errors with details
            for node_error in node_errors:
                if node_error.core_hashes:
                    for core_id, core_hash, core_version in node_error.core_hashes:
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

            return node_errors, verify_errors

    def _handle_final_status(
        self,
        errors_collector: ErrorsCollector,
        result: StressUtilTestResults,
        preparation_result: dict[str, StressUtilDeployResult],
        node_errors: list,
        verify_errors: dict
    ) -> None:
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

        # --- Switch: if cluster_log=all, always attach logs ---
        cluster_log_mode = get_external_param('cluster_log', 'default')
        if cluster_log_mode == 'all' or nodes_with_issues > 0 or workload_errors:
            try:
                errors_collector.attach_nemesis_logs(result.start_time)
            except Exception as e:
                logging.warning(f"Failed to attach nemesis logs: {e}")
            try:
                errors_collector.attach_kikimr_logs(result.start_time, "kikimr")
            except Exception as e:
                logging.warning(f"Failed to attach kikimr logs: {e}")

        # --- FAIL TEST IF CORES OR OOM FOUND ---
        if nodes_with_issues > 0:
            error_msg = f"Test failed: found {nodes_with_issues} issue(s) with coredump(s), OOM(s), VERIFY fail(s) or SAN errors"
            pytest.fail(error_msg)
        # --- MARK TEST AS BROKEN IF WORKLOAD ERRORS (not cores/oom) ---
        if workload_errors:
            raise Exception("Test marked as broken due to workload errors: " + "; ".join(workload_errors))

        # In diagnostic mode don't fail due to coredump/OOM warnings
        if not result.is_all_success() and result.error_message:
            # Create detailed error message with context
            error_details = []
            error_details.append("TEST EXECUTION FAILED: ")
            error_details.append(f"Main error: {result.error_message}")
            if result.stress_util_runs:
                error_details.append("\nExecution details:")
                error_details.append(f"Total iterations attempted: {len(result.iterations)}")
                failed_iterations = []
                successful_iterations = []
                for workload_name, workload_run_info in result.stress_util_runs.items():
                    if workload_run_info.get_successful_runs() == 0:
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
            error_details.append(f"  Total runs: {result.get_total_runs()}")
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
        if result.get_successful_runs() == 0:
            exc = pytest.fail.Exception("All workloads have failed")
            raise exc
        if result.recoverability_result and result.recoverability_result.get_successful_runs() == 0:
            with pytest.warns(RuntimeWarning):
                warnings.warn("All workloads have failed in recovery steps", RuntimeWarning)

    def stop_nemesis_and_check_recoverability(self,
                                              stress_executor: StressRunExecutor,
                                              stress_deployer: StressUtilDeployer,
                                              workload_params: dict[str, dict],
                                              preparation_result: dict[str, StressUtilDeployResult]) -> dict:
        with allure.step("Phase 2.5: Stop nemesis and check recoverability"):
            stress_deployer._manage_nemesis(False, workload_params.keys(), 'RecoverabilityCheck')
            recoverability_execution_result = stress_executor.execute_stress_runs(
                stress_deployer,
                workload_params,
                1200,
                preparation_result,
                False
            )
        return recoverability_execution_result
