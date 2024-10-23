UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_federated_topic)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

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
    ydb/public/sdk/cpp/src/library/json_value
    ydb/public/sdk/cpp/src/library/yson_value
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/persqueue_public/include
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

    ydb/public/sdk/cpp/src/client/topic/codecs
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/impl

    ydb/public/sdk/cpp/src/client/federated_topic
    ydb/public/sdk/cpp/src/client/federated_topic/impl
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage_ut.cpp
)

END()
