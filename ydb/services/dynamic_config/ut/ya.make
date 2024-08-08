UNITTEST_FOR(ydb/services/dynamic_config)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    dynamic_config_ut.cpp
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
