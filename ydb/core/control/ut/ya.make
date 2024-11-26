UNITTEST_FOR(ydb/core/control)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    util
    ydb/core/base
    ydb/core/mind
    ydb/core/mon
    ydb/core/tx/schemeshard
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/services/persqueue_v1
    ydb/services/ydb
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    immediate_control_board_ut.cpp
    immediate_control_board_actor_ut.cpp
)

END()
