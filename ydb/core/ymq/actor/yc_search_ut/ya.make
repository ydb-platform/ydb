UNITTEST()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/ymq/actor
)

SRCS(
    index_events_processor_ut.cpp
    test_events_writer.cpp
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(300)

YQL_LAST_ABI_VERSION()

END()
