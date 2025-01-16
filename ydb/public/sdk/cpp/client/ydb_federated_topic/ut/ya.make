UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_federated_topic)

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
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/public/sdk/cpp/client/ydb_persqueue_public/include
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils

    ydb/public/sdk/cpp/client/ydb_topic/codecs
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_topic/impl

    ydb/public/sdk/cpp/client/ydb_federated_topic
    ydb/public/sdk/cpp/client/ydb_federated_topic/impl
    ydb/public/sdk/cpp/client/ydb_federated_topic/ut/fds_mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage_ut.cpp
)

END()
