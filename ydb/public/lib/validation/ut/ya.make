UNITTEST_FOR(ydb/public/lib/validation)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/lib/validation/ut/protos
)

SRCS(
    ut.cpp
)

END()
