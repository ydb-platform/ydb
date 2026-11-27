UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

SRCS(
    kqp_compute_scheduler_service_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common/simple
    ydb/core/kqp/counters
    ydb/core/kqp/node_service
    ydb/core/kqp/rm_service
    ydb/core/kqp/runtime/scheduler
    ydb/core/kqp/runtime/scheduler/service
    ydb/core/testlib/basics/pg
    ydb/library/actors/interconnect
    ydb/services/workload_manager
    ydb/services/workload_manager/ut/common
    yql/essentials/ast
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
)

YQL_LAST_ABI_VERSION()

END()
