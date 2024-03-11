PY3TEST()

PEERDIR(
    ydb/tests/library
    ydb/library/yql/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
)

END()
