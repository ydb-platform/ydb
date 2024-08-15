UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_value)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/json_value
    ydb/public/sdk/cpp_v2/src/library/yson_value
    ydb/public/sdk/cpp/client/ydb_params
)

SRCS(
    value_ut.cpp
)

END()
