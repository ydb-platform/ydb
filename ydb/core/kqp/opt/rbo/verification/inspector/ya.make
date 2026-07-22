PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cli.py
    plan.py
    trace.py
    witness.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

END()
