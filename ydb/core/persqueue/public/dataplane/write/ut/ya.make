UNITTEST_FOR(ydb/core/persqueue/public/dataplane/write)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    kqp_mock.cpp
    partition_writer_cache_actor_fixture.cpp
    partition_writer_cache_actor_ut.cpp
    pqtablet_mock.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/kqp/common
    ydb/core/kqp/common/simple
    ydb/core/persqueue/public/dataplane
    ydb/core/persqueue/ut/common
    ydb/core/persqueue/writer
    ydb/core/testlib/default
)

END()
