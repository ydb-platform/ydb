UNITTEST_FOR(ydb/core/ydb_convert)

OWNER(g:kikimr)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_convert_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib
)

END()
