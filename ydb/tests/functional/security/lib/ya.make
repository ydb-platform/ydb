PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cluster_config.py
    mon_endpoints_helpers.py
    mon_endpoints_fixtures.py
    security_test_helpers.py
)

PEERDIR(
    ydb/tests/library
)

END()
