PY3_PROGRAM()

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/tools/ydb_serializable/lib
)

PY_SRCS(__main__.py)

END()
