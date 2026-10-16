PY3_PROGRAM(ydb_configure)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tools/cfg
)

END()
