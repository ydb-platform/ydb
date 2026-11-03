UNITTEST_FOR(ydb/core/kqp/runtime/streaming)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

SRCS(
    kqp_streaming_aggregation_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/kqp/runtime
    ydb/core/kqp/runtime/common
    ydb/core/testlib/basics/pg
    yql/essentials/ast
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
