from datetime import datetime
import json
import logging
import time
import traceback
import allure
from typing import Optional
from ydb.tests.olap.lib.allure_utils import NodeErrors
from ydb.tests.library.stability.utils.results_processor import ResultsProcessor
from ydb.tests.library.stability.utils.results_models import StressUtilResult, StressUtilTestResults, RunConfigInfo


def safe_upload_results(
    result: StressUtilTestResults,
    run_config: RunConfigInfo,
    node_errors: list[NodeErrors],
    verify_errors: dict
) -> None:
    """Safely upload test results with error handling and Allure reporting.

    Args:
        result: Stress test results object
        run_config: Run configuration info
        node_errors: List of node error objects
        verify_errors: Dictionary of verification errors
    """
    with allure.step("Upload results to YDB"):
        if not ResultsProcessor.send_results:
            allure.attach("Results upload is disabled (send_results=false)",
                          "Upload status", allure.attachment_type.TEXT)
            return

        try:
            suite_name = 'SingleStressUtil' if len(result.stress_util_runs.keys()) == 1 else 'ParallelStressUtil'
            # Upload aggregated results
            for workload_name, runs in result.stress_util_runs.items():
                _upload_results(runs, run_config, node_errors, verify_errors, suite_name, workload_name)
                with allure.step(f"Process {workload_name} results"):

                    # Informative message about what was uploaded
                    upload_summary = [
                        "Results uploaded successfully:",
                        "• Aggregate results: 1 record (kind=Stability)",
                        f"• Total iterations: {sum(len(node_run.runs) for node_run in runs.node_runs.values())}",
                        f"• Workload: {workload_name}",
                        f"• Suite: {suite_name}",
                    ]
                    allure.attach("\n".join(upload_summary),
                                  f"Upload summary for {workload_name}", allure.attachment_type.TEXT)
        except Exception as e:
            # Log upload error but don't interrupt execution
            error_msg = f"Failed to upload results: {e}\n{traceback.format_exc()}"
            logging.error(error_msg)
            # result.add_warning(error_msg)
            # After adding warning we need to recalculate summary flags
            # Summary flags (with_errors/with_warnings) are automatically added in ydb_cli.py

            # Detailed error info for Allure
            error_details = [
                f"Error type: {type(e).__name__}",
                f"Error message: {error_msg}",
                f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ]

            # Add additional info if this is a YDB error
            if hasattr(e, 'issues'):
                error_details.append(f"YDB issues: {e.issues}")
            if hasattr(e, 'status'):
                error_details.append(f"Status: {e.status}")

            allure.attach("\n".join(error_details),
                          "Upload error details", allure.attachment_type.TEXT)


def _upload_results(
    result: StressUtilResult,
    run_config: RunConfigInfo,
    node_errors: list[NodeErrors],
    verify_errors: dict,
    suite_name: str,
    workload_name: str
) -> None:
    """Upload results for a specific workload test.

    Args:
        result: Stress utility results
        run_config: Run configuration info
        node_errors: List of node errors
        verify_errors: Verification errors dict
        suite_name: Test suite name
        workload_name: Workload name
    """
    stats = {}

    stats["aggregation_level"] = "aggregate"
    stats["run_id"] = ResultsProcessor.get_run_id()
    # Add workload timings for proper analysis
    workload_start_time = result.start_time
    if workload_start_time:
        stats["workload_start_time"] = workload_start_time
        stats["workload_end_time"] = result.end_time
        stats["workload_duration"] = result.end_time - workload_start_time

    stats["total_runs"] = result.get_total_runs()
    stats["successful_runs"] = result.get_successful_runs()
    stats["failed_runs"] = stats["total_runs"] - stats["successful_runs"]
    stats["total_iterations"] = 1
    stats["successful_iterations"] = stats["successful_runs"]
    stats["failed_iterations"] = stats["total_runs"] - stats["successful_runs"]
    stats["planned_duration"] = run_config.duration
    stats["actual_duration"] = result.end_time - result.start_time
    stats["total_execution_time"] = max(run.total_execution_time for run in result.node_runs.values())
    stats["success_rate"] = stats["successful_runs"] / stats["total_runs"]
    # obsolete
    stats["avg_threads_per_iteration"] = 0
    stats["total_threads"] = 1
    stats["use_iterations"] = False

    stats["nodes_percentage"] = run_config.nodes_percentage
    stats["nemesis_enabled"] = run_config.nemesis_enabled
    stats["nemesis"] = run_config.nemesis_enabled
    stats["table_type"] = run_config.table_type
    stats["workload_type"] = workload_name
    stats["test_timestamp"] = result.start_time

    stats["with_warnings"] = False

    aggregated_errors = []
    nodes_with_issues = set()
    had_oom = False
    san_errors_count = 0
    coredump_count = 0

    for error in node_errors:
        nodes_with_issues.add(error.node)
        if len(error.core_hashes) > 0:
            coredump_count += 1
        had_oom |= error.was_oom
        san_errors_count += error.sanitizer_errors

    if san_errors_count > 0:
        aggregated_errors.append(f'SAN errors: {san_errors_count}')
    if coredump_count > 0:
        aggregated_errors.append(f'Collected coredumps: {coredump_count}')
    if had_oom:
        aggregated_errors.append('OOM occurred')

    for error_summary, detailed_info in verify_errors.items():
        aggregated_errors.append(f'VERIFY {error_summary} appeared on')
        for host, hosts_count in detailed_info['hosts_count'].items():
            aggregated_errors.append(f'  {host}: {hosts_count}')

    stats["with_errors"] = len(aggregated_errors) > 0
    stats["node_errors"] = len(aggregated_errors) > 0

    is_success = result.is_all_success() and len(aggregated_errors) == 0
    stats["errors"] = {'other': True} if not is_success else None

    stats["nodes_with_issues"] = list(node.host for node in nodes_with_issues)
    stats["node_error_messages"] = aggregated_errors

    stats["workload_errors"] = None
    stats["workload_warnings"] = None
    stats["workload_error_messages"] = None
    stats["workload_warning_messages"] = None

    end_time = datetime.now().timestamp()

    # Prepare data for upload
    upload_data = {
        "kind": "Stability",
        "suite": suite_name,
        "test": workload_name,
        "timestamp": end_time,
        "is_successful": is_success,
        "statistics": stats,
    }

    # Attach data to Allure report
    allure.attach(
        json.dumps(upload_data, indent=2, default=str),
        f"Aggregate results upload data for {workload_name}",
        allure.attachment_type.JSON
    )

    ResultsProcessor.upload_results(**upload_data)


def test_event_report(
    event_kind: str,
    workload_names: list[str],
    nemesis_enabled: bool,
    verification_phase: Optional[str] = None,
    check_type: Optional[str] = None,
    cluster_issue: Optional[dict] = None
) -> None:
    """
    Universal method for creating test event records in database.

    Args:
        event_kind: Event type ('TestInit', 'ClusterCheck')
        workload_names: List of workload names
        nemesis_enabled: Whether nemesis was enabled
        verification_phase: Verification phase (for ClusterCheck)
        check_type: Check type (for ClusterCheck)
        cluster_issue: Cluster issue information (for ClusterCheck)
    """
    suite_name = 'SingleStressUtil' if len(workload_names) == 1 else 'ParallelStressUtil'
    upload_data = []
    if event_kind == 'TestInit':
        # TestInit - инициализация теста
        statistics = {
            "event_type": "test_initialization",
            "test_started": True
        }

        # Добавляем nemesis_enabled в статистику
        statistics["nemesis_enabled"] = nemesis_enabled

        for workload_name in workload_names:
            upload_data.append({
                "kind": 'TestInit',
                "suite": suite_name,
                "test": workload_name,
                "timestamp": time.time(),
                "is_successful": True,  # TestInit всегда успешен
                "statistics": statistics
            })

        allure_title = f"Test initialization for {len(workload_names)} stress utils"

    elif event_kind == 'ClusterCheck':
        # ClusterCheck - проверка кластера
        if cluster_issue is None:
            raise ValueError("cluster_issue is required for ClusterCheck events")

        is_successful = cluster_issue.get("issue_type") is None

        stats = {
            "verification_phase": verification_phase,
            "check_type": check_type,
            **cluster_issue
        }

        # Добавляем nemesis_enabled в статистику
        stats["nemesis_enabled"] = nemesis_enabled

        for workload_name in workload_names:
            upload_data.append({
                "kind": 'ClusterCheck',
                "suite": suite_name,
                "test": workload_name,
                "timestamp": time.time(),
                "is_successful": is_successful,
                "statistics": stats
            })

        allure_title = f"Cluster check results for {verification_phase} - {len(workload_names)} stress utils"

    else:
        raise ValueError(f"Unknown event_kind: {event_kind}")

    # Прикрепляем данные к Allure отчету
    allure.attach(
        json.dumps(upload_data, indent=2, default=str),
        allure_title,
        allure.attachment_type.JSON
    )
    for data in upload_data:
        ResultsProcessor.upload_results(**data)
