UNITTEST_FOR(ydb/core/mind/hive)

IF (OS_LINUX)
    # The integrated test binary can exceed the DWARF32 offset range.
    LINKER_SCRIPT(${ARCADIA_ROOT}/ydb/apps/ydbd/strip_debug.ld)
ENDIF()

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

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
    event_history_ut.cpp
    object_distribution_ut.cpp
    scale_recommender_policy_ut.cpp
    sequencer_ut.cpp
    storage_pool_info_ut.cpp
    tenants_ut.cpp
    hive_ut.cpp
    hive_impl_ut.cpp
    ut_common.cpp
    ut_common.h
)

END()
