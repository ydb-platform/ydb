UNITTEST_FOR(ydb/core/actorlib_impl)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SPLIT_FACTOR(20)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/apps/version
    ydb/library/actors/core
    ydb/library/actors/interconnect
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/testlib/actors
    ydb/core/testlib/basics/default
    yql/essentials/minikql/comp_nodes/llvm14
)

SRCS(
    actor_activity_ut.cpp
    actor_bootstrapped_ut.cpp
    actor_tracker_ut.cpp
    test_interconnect_ut.cpp
    test_protocols_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
