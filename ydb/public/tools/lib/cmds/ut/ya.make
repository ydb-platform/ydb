PY3TEST()

PEERDIR(
    ydb/public/tools/lib/cmds
    ydb/tools/cfg
    yql/essentials/providers/common/proto
)

TEST_SRCS(
    test.py
)

END()
