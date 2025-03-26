UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/value
    ydb/public/sdk/cpp/src/client/params
)

SRCS(
    value_ut.cpp
)

END()
