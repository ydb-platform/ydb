UNITTEST_FOR(ydb/core/persqueue/writer)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

SRCS(
    partition_chooser_actor_scenarios_ut.cpp
    partition_chooser_exception_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/kqp/common/events
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/writer
    ydb/core/testlib/default
    ydb/library/persqueue/topic_parser
    ydb/services/metadata
)

END()
