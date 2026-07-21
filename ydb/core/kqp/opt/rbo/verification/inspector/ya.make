PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cli.py
    plan.py
    trace.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

END()
