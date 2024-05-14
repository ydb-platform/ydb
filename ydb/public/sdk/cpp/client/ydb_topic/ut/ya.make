UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_topic)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/testlib/default
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/public/sdk/cpp/client/ydb_persqueue_public/impl
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils

    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_topic/include
    ydb/public/sdk/cpp/client/ydb_topic/common
    ydb/public/sdk/cpp/client/ydb_topic/impl
    ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils

    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/persqueue/ut/common
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage_ut.cpp
    describe_topic_ut.cpp
    local_partition_ut.cpp
    topic_to_table_ut.cpp
    trace_ut.cpp
)

END()
