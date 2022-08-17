UNITTEST_FOR(ydb/public/lib/validation)

OWNER(
    ilnaz
    g:kikimr
)

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
