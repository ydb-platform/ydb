PY3_PROGRAM(ydb-canondata-sync)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/tools/canondata_sync/lib
)

END()
