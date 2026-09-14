UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    row_ranges_public_include_ut.cpp
    row_ranges_ut.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/row_ranges
)

END()
