UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_persqueue_public)

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
    ydb/public/sdk/cpp/client/ydb_persqueue_public/impl
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_topic/codecs
    ydb/public/sdk/cpp/client/ydb_topic/impl
)

YQL_LAST_ABI_VERSION()

SRCS(
    common_ut.cpp
    read_session_ut.cpp
    basic_usage_ut.cpp
    compress_executor_ut.cpp
    compression_ut.cpp
    retry_policy_ut.cpp
)

END()

RECURSE_FOR_TESTS(
    with_offset_ranges_mode_ut
)
