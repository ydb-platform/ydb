UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    result_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/params
)

END()
