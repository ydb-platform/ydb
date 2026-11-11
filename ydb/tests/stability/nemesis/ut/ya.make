PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_chaos_target.py
    test_boundary_scheduler.py
    test_recovery_probe.py
    test_command_dispatch.py
    test_chaos_problems.py
    test_catalog_annotations.py
    test_guard_invariants.py
    test_pile_budget.py
    test_fault_engine.py
    test_orchestrator_api.py
    test_operations_api.py
    test_metrics.py
    test_local_kikimr.py
    test_local_process.py
)

PEERDIR(
    ydb/tests/stability/nemesis
    contrib/python/PyYAML
    contrib/python/pytest
    contrib/python/Flask
)

END()
