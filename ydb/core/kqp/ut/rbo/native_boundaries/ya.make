UNITTEST_FOR(ydb/core/kqp/opt/rbo)

FORK_SUBTESTS()

SRCS(
    kqp_native_struct_ut.cpp
    kqp_native_bindings_ut.cpp
    kqp_native_workload_ut.cpp
    kqp_native_layout_ut.cpp
    kqp_native_expression_arena_ut.cpp
    kqp_native_scalar_ut.cpp
    kqp_native_range_policy_ut.cpp
    kqp_native_lookup_admission_ut.cpp
    kqp_native_predicate_admission_ut.cpp
    kqp_native_olap_policy_ut.cpp
    kqp_native_reference_lowering_ut.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/kqp/opt
    yql/essentials/minikql/comp_nodes
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/arrow_resolve
    yql/essentials/minikql/invoke_builtins
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/re2
)

RESOURCE(
    ../data/schema/clickbench.sql clickbench/schema.sql
    ../data/yql-clickbench/q20.yql clickbench/q20.yql
    ../data/yql-clickbench/q25.yql clickbench/q25.yql
    ../data/yql-clickbench/q26.yql clickbench/q26.yql
    ../data/yql-clickbench/q27.yql clickbench/q27.yql
)

YQL_LAST_ABI_VERSION()

END()
