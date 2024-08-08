UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_federated_topic)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/testlib/default
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/public/sdk/cpp/client/ydb_persqueue_public/include
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils

    ydb/public/sdk/cpp/client/ydb_topic/codecs
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_topic/impl

    ydb/public/sdk/cpp/client/ydb_federated_topic
    ydb/public/sdk/cpp/client/ydb_federated_topic/impl
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage_ut.cpp
)

END()
