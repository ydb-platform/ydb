PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cluster_config.py
    security_test_helpers.py
)

PEERDIR(
    ydb/tests/library
)

END()
