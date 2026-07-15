GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/value
    ydb/public/sdk/cpp/src/client/params
)

SRCS(
    value_ut.cpp
)

END()
