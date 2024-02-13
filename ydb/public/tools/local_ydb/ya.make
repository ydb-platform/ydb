PY3_PROGRAM(local_ydb)

PY_SRCS(__main__.py)

PEERDIR(
    ydb/library/yql/providers/common/proto
    ydb/public/tools/lib/cmds
)

END()
