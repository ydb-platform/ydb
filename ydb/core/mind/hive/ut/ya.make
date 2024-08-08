UNITTEST_FOR(ydb/core/mind/hive)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/actors/helpers
    ydb/core/base
    ydb/core/mind
    ydb/core/mind/hive
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    object_distribution_ut.cpp
    sequencer_ut.cpp
    storage_pool_info_ut.cpp
    hive_ut.cpp
    hive_impl_ut.cpp
)

END()
