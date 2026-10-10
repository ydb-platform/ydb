UNITTEST_FOR(ydb/public/lib/validation)

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
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
