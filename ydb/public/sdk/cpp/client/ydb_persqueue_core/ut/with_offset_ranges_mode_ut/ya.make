UNITTEST()

IF (SANITIZER_TYPE)
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
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

ENV(PQ_OFFSET_RANGES_MODE="1")

SRCDIR(
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut
    ydb/public/sdk/cpp/client/ydb_persqueue_core
)

SRCS(
    common_ut.cpp
    read_session_ut.cpp
    basic_usage_ut.cpp
    compress_executor_ut.cpp
    retry_policy_ut.cpp
)

END()
