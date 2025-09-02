UNITTEST_FOR(ydb/core/testlib/actors)

FORK_SUBTESTS()
IF (SANITIZER_TYPE OR WITH_VALGRIND)
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
