PY3TEST()

PEERDIR(
    ydb/tests/library
    yql/essentials/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
)

END()
