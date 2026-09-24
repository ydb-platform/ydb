UNITTEST_FOR(ydb/services/dynamic_config)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    dynamic_config_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/services/cms
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

END()
