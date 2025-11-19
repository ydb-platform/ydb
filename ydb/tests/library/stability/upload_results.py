from datetime import datetime
import json
import logging
import allure
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.library.stability.aggregate_results import StressUtilTestResults


def safe_upload_results(self, result: StressUtilTestResults, workload_name):
    """Safe results upload with error handling and Allure reporting"""
    with allure.step("Upload results to YDB"):
        if not ResultsProcessor.send_results:
            allure.attach("Results upload is disabled (send_results=false)",
                          "Upload status", allure.attachment_type.TEXT)
            return

        try:
            # Upload aggregated results
            self._upload_results(result, workload_name)

            # Upload per-run results
            per_run_count = sum(len(getattr(iteration, "runs", [iteration]))
                                for iteration in result.iterations.values())
            self._upload_results_per_workload_run(result, workload_name)

            # Informative message about what was uploaded
            upload_summary = [
                "Results uploaded successfully:",
                "• Aggregate results: 1 record (kind=Stability)",
                f"• Per-run results: {per_run_count} records (kind=Stability)",
                f"• Total iterations: {len(result.iterations)}",
                f"• Workload: {workload_name}",
                f"• Suite: {type(self).suite()}",
            ]
            allure.attach("\n".join(upload_summary),
                          "Upload summary", allure.attachment_type.TEXT)
        except Exception as e:
            # Log upload error but don't interrupt execution
            error_msg = f"Failed to upload results: {e}"
            logging.error(error_msg)
            result.add_warning(error_msg)
            # After adding warning we need to recalculate summary flags
            # Summary flags (with_errors/with_warnings) are automatically added in ydb_cli.py

            # Detailed error info for Allure
            error_details = [
                f"Error type: {type(e).__name__}",
                f"Error message: {str(e)}",
                f"Timestamp: {datetime.strftime('%Y-%m-%d %H:%M:%S')}",
            ]

            # Add additional info if this is a YDB error
            if hasattr(e, 'issues'):
                error_details.append(f"YDB issues: {e.issues}")
            if hasattr(e, 'status'):
                error_details.append(f"Status: {e.status}")

            allure.attach("\n".join(error_details),
                          "Upload error details", allure.attachment_type.TEXT)


def _upload_results(self, result: StressUtilTestResults, workload_name):
    """Overridden method for workload tests"""
    stats = {}
    stats = result.get_stats(workload_name)
    if stats is not None:
        stats["aggregation_level"] = "aggregate"
        stats["run_id"] = ResultsProcessor.get_run_id()
        # Add workload timings for proper analysis
        workload_start_time = getattr(result, 'workload_start_time', None)
        if workload_start_time:
            stats["workload_start_time"] = workload_start_time
            # Workload end time = time before diagnostics start
            workload_end_time = workload_start_time + stats.get("total_execution_time", 0)
            stats["workload_end_time"] = workload_end_time
            stats["workload_duration"] = stats.get("total_execution_time", 0)

    end_time = datetime.time()

    # Prepare data for upload
    upload_data = {
        "kind": "Stability",
        "suite": type(self).suite(),
        "test": workload_name,
        "timestamp": end_time,
        "is_successful": result.success,
        "statistics": stats,
    }

    # Attach data to Allure report
    allure.attach(
        json.dumps(upload_data, indent=2, default=str),
        f"Aggregate results upload data for {workload_name}",
        allure.attachment_type.JSON
    )

    ResultsProcessor.upload_results(**upload_data)


def _upload_results_per_workload_run(workload_name, result):
    """Overridden method for workload tests"""
    suite = workload_name
    agg_stats = result.get_stats(workload_name)
    nemesis_enabled = agg_stats.get(
        "nemesis_enabled") if agg_stats else None
    run_id = ResultsProcessor.get_run_id()
    for iter_num, iteration in result.iterations.items():
        runs = getattr(iteration, "runs", None) or [iteration]
        for run_idx, run in enumerate(runs):
            if getattr(run, "error_message", None):
                resolution = "error"
            elif getattr(run, "warning_message", None):
                resolution = "warning"
            elif hasattr(run, "timeout") and run.timeout:
                resolution = "timeout"
            else:
                resolution = "ok"

            stats = {
                "iteration": iter_num,
                "run_index": run_idx,
                "duration": getattr(run, "time", None),
                "resolution": resolution,
                "error_message": getattr(run, "error_message", None),
                "warning_message": getattr(run, "warning_message", None),
                "nemesis_enabled": nemesis_enabled,
                "aggregation_level": "per_run",
                "run_id": run_id,
            }

            # Prepare per-run upload data
            per_run_upload_data = {
                "kind": "Stability",
                "suite": suite,
                "test": f"{workload_name}__iter_{iter_num}__run_{run_idx}",
                "timestamp": datetime.time(),
                "is_successful": (resolution == "ok"),
                "duration": stats["duration"],
                "statistics": stats,
            }

            # Attach data to Allure report (only for first few runs to avoid clutter)
            if iter_num <= 2 and run_idx <= 2:  # Ограничиваем количество attach'ей
                allure.attach(
                    json.dumps(per_run_upload_data, indent=2, default=str),
                    f"Per-run results upload data: {workload_name} iter_{iter_num} run_{run_idx}",
                    allure.attachment_type.JSON
                )

            ResultsProcessor.upload_results(**per_run_upload_data)
