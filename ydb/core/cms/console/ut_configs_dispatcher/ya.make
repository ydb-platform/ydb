UNITTEST_FOR(ydb/core/cms/console)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    library/cpp/monlib/service
    library/cpp/protobuf/util
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    configs_cache_ut.cpp
    configs_dispatcher_ut.cpp
)

END()

