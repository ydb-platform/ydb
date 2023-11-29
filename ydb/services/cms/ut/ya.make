UNITTEST_FOR(ydb/services/cms)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    cms_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/services/cms
)

YQL_LAST_ABI_VERSION()

END()
