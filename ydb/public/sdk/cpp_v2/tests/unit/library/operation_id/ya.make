UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    operation_id_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/sdk/cpp_v2/src/library/operation_id
)

END()
