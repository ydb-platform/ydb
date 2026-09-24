PY3TEST()

SIZE(MEDIUM)
FORK_SUBTESTS()
SPLIT_FACTOR(8)

TEST_SRCS(
    test_ydb_bench.py
    test_ydb_telemetry.py
    test_monitoring_settings.py
    test_metrics_export.py
    test_grafana.py
    test_hosts.py
    test_run_index.py
    test_cluster_templates.py
    test_cluster_config.py
    test_cluster_deployment.py
    test_distributed_sessions.py
    test_distributed_plan.py
    test_distributed_builder.py
    test_process_recovery.py
    test_distributed_worker.py
    test_distributed_disks.py
    test_distributed_coordinator.py
    test_distributed_telemetry.py
    test_distributed_reports.py
)

PEERDIR(
    ydb/tools/ydb_bench/benchmarks
    ydb/tools/ydb_bench/lib
)

END()
