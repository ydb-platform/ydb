UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

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
    ydb/public/sdk/cpp_v2/src/client/driver
    ydb/public/sdk/cpp_v2/src/client/table
)

SRCS(
    driver_ut.cpp
)

END()
