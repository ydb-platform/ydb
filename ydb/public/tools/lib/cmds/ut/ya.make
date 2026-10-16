PY3TEST()

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/public/tools/lib/cmds
    yql/essentials/providers/common/proto
)

TEST_SRCS(
    test.py
)

END()
