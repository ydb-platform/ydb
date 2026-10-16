UNITTEST_FOR(ydb/core/testlib/actors)

FORK_SUBTESTS()
REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
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
