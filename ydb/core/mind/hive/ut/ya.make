UNITTEST_FOR(ydb/core/mind/hive)

OWNER(
    ddoarn
    xenoxeno
    g:kikimr
)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/mind
    ydb/core/mind/hive
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    sequencer_ut.cpp 
    storage_pool_info_ut.cpp 
    hive_ut.cpp 
    hive_impl_ut.cpp 
)

END()
