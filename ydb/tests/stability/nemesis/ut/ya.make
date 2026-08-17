PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_chaos_target.py
    test_boundary_scheduler.py
    test_recovery_probe.py
    test_schedule_loop.py
    test_chaos_problems.py
    test_catalog_annotations.py
    test_guard_invariants.py
    test_orchestrator_api.py
    test_metrics.py
)

PEERDIR(
    ydb/tests/stability/nemesis
    contrib/python/PyYAML
    contrib/python/pytest
    contrib/python/Flask
)

END()
