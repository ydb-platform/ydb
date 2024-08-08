UNITTEST_FOR(ydb/public/lib/operation_id)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    operation_id_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
