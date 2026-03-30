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
    internal/master/install.py
    internal/master/orchestrator_warden_checker.py
    internal/master/nemesis/schedule_loop.py
    internal/master/nemesis/chaos_state.py
    internal/master/nemesis/default_planner.py
    internal/master/nemesis/kill_node_planner.py
    internal/master/nemesis/network_planner.py
    internal/master/nemesis/nemesis_planner_base.py
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
