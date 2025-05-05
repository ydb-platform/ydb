PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    yql/essentials/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
)

END()
