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

SRCS(
    endpoints_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_endpoints
)

END()
