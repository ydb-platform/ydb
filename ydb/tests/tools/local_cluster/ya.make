PY3_PROGRAM(local_cluster)

PY_SRCS(
    __main__.py
    nemesis_integration.py
)

BUNDLE(
    ydb/tests/stability/nemesis NAME nemesis
)

RESOURCE(
    nemesis nemesis
)

PEERDIR(
    ydb/tests/library
    library/python/resource
)

END()
