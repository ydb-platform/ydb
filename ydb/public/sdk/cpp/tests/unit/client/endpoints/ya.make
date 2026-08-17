UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    endpoints_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/endpoints
)

END()
