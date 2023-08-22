PY3_PROGRAM(local_ydb)

INCLUDE(${ARCADIA_ROOT}/ydb/opensource.inc)


PY_SRCS(__main__.py)

PEERDIR(
    ydb/public/tools/lib/cmds
)

END()
