PY3_LIBRARY()

PY_SRCS(
    __init__.py
    case.py
    cli.py
    materialize.py
    model.py
    observation.py
    runner.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
    ydb/core/kqp/opt/rbo/verification/inspector
)

END()
