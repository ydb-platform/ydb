UNITTEST_FOR(ydb/library/yql/dq/comp_nodes)

SIZE(MEDIUM)

PEERDIR(
    ydb/library/yql/dq/comp_nodes
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/llvm16

    ydb/core/kqp/runtime

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)

YQL_LAST_ABI_VERSION()

SRCS(
    dq_factories.cpp

    dq_hash_combine_ut.cpp
    dq_block_hash_join_ut.cpp
    bench.cpp
)

END()
