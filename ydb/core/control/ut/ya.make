UNITTEST_FOR(ydb/core/control)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    library/cpp/testing/unittest
    util
    ydb/core/base
    ydb/core/mind
    ydb/core/mon
    yql/essentials/sql/pg_dummy
    ydb/services/ydb
    ydb/services/persqueue_v1
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
)

SRCS(
    immediate_control_board_actor_ut.cpp
)

END()
