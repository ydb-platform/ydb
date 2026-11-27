PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_chaos_target.py
    test_chaos_state.py
    test_boundary_scheduler.py
    test_recovery_probe.py
    test_schedule_loop.py
    test_chaos_problems.py
    test_catalog_annotations.py
    test_guard_invariants.py
    test_orchestrator_api.py
    test_metrics.py
    test_local_kikimr.py
    test_cluster_context.py
    test_healthcheck_endpoints.py
    test_local_cluster_view.py
    test_local_harness_controller.py
    test_local_control_proxy.py
    test_local_disk_runner.py
)

PEERDIR(
    ydb/tests/stability/nemesis
    contrib/python/PyYAML
    contrib/python/pytest
    contrib/python/Flask
)

END()
