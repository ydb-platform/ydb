UNITTEST_FOR(ydb/core/testlib/actors)

FORK_SUBTESTS()
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(300)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/regex/pcre
)

SRCS(
    test_runtime_ut.cpp
)

END()
