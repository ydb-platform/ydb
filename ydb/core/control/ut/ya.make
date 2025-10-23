UNITTEST_FOR(ydb/core/control)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/mind
    ydb/core/mon
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/services/persqueue_v1
    ydb/services/ydb
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
)

SRCS(
    immediate_control_board_actor_ut.cpp
)

END()
