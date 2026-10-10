UNITTEST_FOR(ydb/core/persqueue/public/cluster_tracker)

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
PEERDIR(
)

SRCS(
    cluster_select_ut.cpp
)

END()
