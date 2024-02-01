UNITTEST_FOR(ydb/core/driver_lib/validation)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/driver_lib/validation/ut/protos
)

SRCS(
    ut.cpp
)

END()
