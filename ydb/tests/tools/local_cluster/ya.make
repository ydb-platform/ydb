PY3_PROGRAM(local_cluster)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/library
)

END()

