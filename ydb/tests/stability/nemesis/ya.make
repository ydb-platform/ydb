PY3_PROGRAM()

PY_SRCS(
    __main__.py
    internal/install.py
    internal/config.py
    internal/models.py
    internal/nemesis/catalog.py
    internal/nemesis/runner.py
    internal/event_loop.py
    routers/agent_router.py
    routers/orchestrator_router.py
    internal/agent_warden_checker.py
    internal/orchestrator_warden_checker.py
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

