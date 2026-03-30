PY3_PROGRAM()

PY_SRCS(
    __main__.py
    internal/config.py
    internal/warden_catalog.py
    internal/models.py
    internal/event_loop.py
    internal/nemesis/catalog.py
    internal/nemesis/chaos_dispatch.py
    internal/agent/agent_warden_checker.py
    internal/agent/agent_safety_runs.py
    internal/agent/nemesis/runner.py
    internal/orchestrator/install.py
    internal/orchestrator/orchestrator_warden_checker.py
    internal/orchestrator/nemesis/schedule_loop.py
    internal/orchestrator/nemesis/chaos_state.py
    internal/orchestrator/nemesis/default_planner.py
    internal/orchestrator/nemesis/kill_node_planner.py
    internal/orchestrator/nemesis/network_planner.py
    internal/orchestrator/nemesis/nemesis_planner_base.py
    routers/agent_router.py
    routers/orchestrator_router.py
    app.py
)

DEPENDS(
    ydb/tools/cfg/bin
    ydb/tests/tools/nemesis/driver
)

PEERDIR(
    ydb/tests/tools/nemesis/library
    ydb/tests/library
    ydb/tests/library/stability
    ydb/tests/library/wardens
    contrib/python/Flask
)

END()
