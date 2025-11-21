from datetime import datetime
import json
import logging
import allure
from ydb.tests.olap.lib.allure_utils import NodeErrors
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.library.stability.aggregate_results import StressUtilResult, StressUtilTestResults, RunConfigInfo


def safe_upload_results(result: StressUtilTestResults, run_config: RunConfigInfo, node_errors: list[NodeErrors], verify_errors):
    """Safe results upload with error handling and Allure reporting"""
    with allure.step("Upload results to YDB"):
        # if not ResultsProcessor.send_results:
        #     allure.attach("Results upload is disabled (send_results=false)",
        #                   "Upload status", allure.attachment_type.TEXT)
        #     return

        try:
            suite_name = 'SingleStressUtil' if len(result.stress_util_runs.keys()) == 1 else 'ParallelStressUtil'
            # Upload aggregated results
            for workload_name, runs in result.stress_util_runs.items():
                _upload_results(runs, run_config, node_errors, verify_errors, suite_name, workload_name)

                # Informative message about what was uploaded
                upload_summary = [
                    "Results uploaded successfully:",
                    "• Aggregate results: 1 record (kind=Stability)",
                    f"• Total iterations: {sum(len(node_run.runs) for node_run in runs.node_runs.values())}",
                    f"• Workload: {workload_name}",
                    f"• Suite: {suite_name}",
                ]
                allure.attach("\n".join(upload_summary),
                              f"Upload summary [{workload_name}]", allure.attachment_type.TEXT)
        except Exception as e:
            # Log upload error but don't interrupt execution
            error_msg = f"Failed to upload results: {e}"
            logging.error(error_msg)
            # result.add_warning(error_msg)
            # After adding warning we need to recalculate summary flags
            # Summary flags (with_errors/with_warnings) are automatically added in ydb_cli.py

            # Detailed error info for Allure
            error_details = [
                f"Error type: {type(e).__name__}",
                f"Error message: {str(e)}",
                f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ]

            # Add additional info if this is a YDB error
            if hasattr(e, 'issues'):
                error_details.append(f"YDB issues: {e.issues}")
            if hasattr(e, 'status'):
                error_details.append(f"Status: {e.status}")

            allure.attach("\n".join(error_details),
                          "Upload error details", allure.attachment_type.TEXT)


def _upload_results(result: StressUtilResult, run_config: RunConfigInfo, node_errors: list[NodeErrors], verify_errors, suite_name, workload_name):
    """Overridden method for workload tests"""
    stats = {}
    # stats = result.get_stats(workload_name)
    if stats is not None:
        stats["aggregation_level"] = "aggregate"
        stats["run_id"] = ResultsProcessor.get_run_id()
        # Add workload timings for proper analysis
        workload_start_time = result.start_time
        if workload_start_time:
            stats["workload_start_time"] = workload_start_time
            stats["workload_end_time"] = result.end_time
            stats["workload_duration"] = result.total_execution_time

        stats["total_runs"] = result.get_total_runs()
        stats["successful_runs"] = result.get_successful_runs()
        stats["failed_runs"] = stats["total_runs"] - stats["successful_runs"]
        stats["total_iterations"] = stats["total_runs"]
        stats["successful_iterations"] = stats["successful_runs"]
        stats["failed_iterations"] = stats["total_runs"] - stats["successful_runs"]
        stats["planned_duration"] = run_config.duration
        stats["actual_duration"] = result.end_time - result.start_time
        stats["total_execution_time"] = sum(run.total_execution_time for run in result.node_runs.values())
        stats["success_rate"] = stats["successful_runs"] / stats["total_runs"]
        # obsolete
        stats["avg_threads_per_iteration"] = 0
        stats["total_threads"] = 0
        stats["use_iterations"] = False

        stats["nodes_percentage"] = run_config.nodes_percentage
        stats["nemesis_enabled"] = run_config.nemesis_enabled
        stats["nemesis"] = run_config.nemesis_enabled
        stats["table_type"] = run_config.table_type
        stats["workload_type"] = workload_name
        stats["test_timestamp"] = result.start_time

        stats["errors"] = {'other': True} if not result.is_all_success() else None
        stats["with_errors"] = not result.is_all_success()
        stats["with_warnings"] = False

        aggregated_errors = []
        nodes_with_issues = set()
        for error in node_errors:
            nodes_with_issues.add(error.node)
            aggregated_errors.append(error.message)

        for error_summary, detailed_info in verify_errors.items():
            aggregated_errors.append(f'VERIFY {error_summary} appeared on')
            for host, host_count in detailed_info['host_count']:
                aggregated_errors.append(f'  {host}: {host_count}')

        stats["node_errors"] = aggregated_errors

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
        "is_successful": result.is_all_success(),
        "statistics": stats,
    }

    # Attach data to Allure report
    allure.attach(
        json.dumps(upload_data, indent=2, default=str),
        f"Aggregate results upload data for {workload_name}",
        allure.attachment_type.JSON
    )

    ResultsProcessor.upload_results(**upload_data)
