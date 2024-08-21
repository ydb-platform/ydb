PY3TEST()

PEERDIR(
    ydb/public/tools/lib/cmds
    ydb/library/yql/providers/common/proto
)

TEST_SRCS(
    test.py
)

DATA(
    ydb/ydb/public/tools/lib/cmds/ut/config.yaml
    ydb/ydb/public/tools/lib/cmds/ut/patch.yaml
)

END()
