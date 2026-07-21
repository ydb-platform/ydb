PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cli.py
    model.py
    runner.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
    ydb/core/kqp/opt/rbo/verification/inspector
)

END()
