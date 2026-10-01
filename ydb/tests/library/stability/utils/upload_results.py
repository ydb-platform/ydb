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
) -> None:
    """Safely upload test results with error handling and Allure reporting.

    Args:
        result: Stress test results object
        run_config: Run configuration info
        node_errors: List of node error objects
    """
    with allure.step("Upload results to YDB"):
        if not ResultsProcessor.send_results:
            allure.attach("Results upload is disabled (send_results=false)",
                          "Upload status", allure.attachment_type.TEXT)
            return

        try:
            suite_name = 'SingleStressUtil' if len(result.stress_util_runs.keys()) == 1 else 'ParallelStressUtil'
            recoverability = result.recoverability_result
            # Upload aggregated results
            for workload_name, runs in result.stress_util_runs.items():
                recoverability_runs = None
                if recoverability is not None and workload_name in recoverability.stress_util_runs:
                    recoverability_runs = recoverability.stress_util_runs[workload_name]
                _upload_results(
                    runs,
                    run_config,
                    node_errors,
                    suite_name,
                    workload_name,
                    recoverability_runs=recoverability_runs,
                )
                with allure.step(f"Process {workload_name} results"):
                    phases_count = 2 if recoverability_runs is not None else 1
                    upload_summary = [
                        "Results uploaded successfully:",
                        "• Aggregate results: 1 record (kind=Stability)",
                        f"• Phases: {phases_count}",
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


def _build_phase_stats(
    phase_name: str,
    phase_result: StressUtilResult,
    nemesis_enabled: bool,
    planned_duration: Optional[float] = None,
) -> dict:
    """Build Stats fragment for a single workload execution phase.

    Args:
        phase_name: Phase identifier ('main' or 'recoverability')
        phase_result: Results for this phase
        nemesis_enabled: Whether nemesis was active during the phase
        planned_duration: Planned duration in seconds (optional)

    Returns:
        dict: Phase statistics without per-host breakdown
    """
    total_runs = phase_result.get_total_runs()
    successful_runs = phase_result.get_successful_runs()
    failed_runs = total_runs - successful_runs
    actual_duration = None
    if phase_result.start_time is not None and phase_result.end_time is not None:
        actual_duration = phase_result.end_time - phase_result.start_time

    total_execution_time = None
    if phase_result.node_runs:
        total_execution_time = max(
            (run.total_execution_time or 0) for run in phase_result.node_runs.values()
        )

    success_rate = (successful_runs / total_runs) if total_runs else 0.0

    phase = {
        "name": phase_name,
        "nemesis_enabled": nemesis_enabled,
        "start_time": phase_result.start_time,
        "end_time": phase_result.end_time,
        "total_runs": total_runs,
        "successful_runs": successful_runs,
        "failed_runs": failed_runs,
        "success_rate": success_rate,
        "hosts_count": len(phase_result.node_runs),
        "is_successful": phase_result.is_all_success() and total_runs > 0,
    }
    if planned_duration is not None:
        phase["planned_duration"] = planned_duration
    if actual_duration is not None:
        phase["actual_duration"] = actual_duration
    if total_execution_time is not None:
        phase["total_execution_time"] = total_execution_time
    return phase


def _upload_results(
    result: StressUtilResult,
    run_config: RunConfigInfo,
    node_errors: list[NodeErrors],
    suite_name: str,
    workload_name: str,
    recoverability_runs: Optional[StressUtilResult] = None,
) -> None:
    """Upload results for a specific workload test.

    Top-level Stats keep main-phase aggregates for backward compatibility.
    All execution periods (main with/without nemesis, recoverability) are
    listed in ``phases`` so dashboards can distinguish them without schema changes.

    Args:
        result: Stress utility results (main phase)
        run_config: Run configuration info
        node_errors: List of node errors
        suite_name: Test suite name
        workload_name: Workload name
        recoverability_runs: Optional recoverability-phase results (nemesis off)
    """
    stats = {}

    stats["aggregation_level"] = "aggregate"
    stats["run_id"] = ResultsProcessor.get_run_id()
    # Add workload timings for proper analysis (main phase)
    workload_start_time = result.start_time
    if workload_start_time:
        stats["workload_start_time"] = workload_start_time
        stats["workload_end_time"] = result.end_time
        stats["workload_duration"] = result.end_time - workload_start_time

    stats["total_runs"] = result.get_total_runs()
    stats["successful_runs"] = result.get_successful_runs()
    stats["failed_runs"] = stats["total_runs"] - stats["successful_runs"]
    stats["total_iterations"] = stats["total_runs"]
    stats["successful_iterations"] = stats["successful_runs"]
    stats["failed_iterations"] = stats["failed_runs"]
    stats["planned_duration"] = run_config.duration
    stats["actual_duration"] = result.end_time - result.start_time if result.start_time and result.end_time else None
    if result.node_runs:
        stats["total_execution_time"] = max(
            (run.total_execution_time or 0) for run in result.node_runs.values()
        )
    else:
        stats["total_execution_time"] = 0
    stats["success_rate"] = (
        stats["successful_runs"] / stats["total_runs"] if stats["total_runs"] else 0.0
    )
    # obsolete
    stats["avg_threads_per_iteration"] = 0
    stats["total_threads"] = 1
    stats["use_iterations"] = stats["total_runs"] > 1

    stats["nodes_percentage"] = run_config.nodes_percentage
    stats["nemesis_enabled"] = run_config.nemesis_enabled
    stats["nemesis"] = run_config.nemesis_enabled
    stats["table_type"] = run_config.table_type
    stats["workload_type"] = workload_name
    stats["test_timestamp"] = result.start_time
    stats["hosts_count"] = len(result.node_runs)

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

    # Phases: main (with or without nemesis) + optional recoverability (always without)
    phases = [
        _build_phase_stats(
            phase_name="main",
            phase_result=result,
            nemesis_enabled=bool(run_config.nemesis_enabled),
            planned_duration=run_config.duration,
        )
    ]
    if recoverability_runs is not None:
        phases.append(
            _build_phase_stats(
                phase_name="recoverability",
                phase_result=recoverability_runs,
                nemesis_enabled=False,
                planned_duration=1200,
            )
        )
    stats["phases"] = phases
    stats["phases_count"] = len(phases)

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
