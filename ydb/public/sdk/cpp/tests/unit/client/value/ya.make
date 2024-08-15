UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

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
    ydb/public/sdk/cpp/src/library/json_value
    ydb/public/sdk/cpp/src/library/yson_value
    ydb/public/sdk/cpp/src/client/value
    ydb/public/sdk/cpp/src/client/params
)

SRCS(
    value_ut.cpp
)

END()
