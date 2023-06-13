UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_value)

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
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_params
)

SRCS(
    value_ut.cpp
)

END()
