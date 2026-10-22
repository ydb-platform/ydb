PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cli.py
    driver.py
    input.py
    model.py
    protocol.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

END()
