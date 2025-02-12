UNITTEST_FOR(ydb/public/sdk/cpp/src/client/topic)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/testlib/default
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/persqueue_public/impl
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/common
    ydb/public/sdk/cpp/src/client/topic/impl
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils

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
