UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(16)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_rbo_compatibility_ut.cpp
    kqp_rbo_copy_ut.cpp
    kqp_rbo_decorrelation_ut.cpp
    kqp_rbo_global_ut.cpp
    kqp_rbo_lowering_ut.cpp
    kqp_rbo_lookup_join_ut.cpp
    kqp_rbo_olap_ut.cpp
    kqp_rbo_yql_ut.cpp
    kqp_native_document_ut.cpp
    kqp_native_catalog_ut.cpp
    kqp_native_aggregate_filter_ut.cpp
    kqp_native_compilation_time_ut.cpp
    kqp_native_olap_ut.cpp
    kqp_native_effects_ut.cpp
    kqp_native_join_using_ut.cpp
    kqp_native_olap_policy_ut.cpp
    kqp_native_execution_ut.cpp
    kqp_native_reference_execution_ut.cpp
    kqp_native_reference_join_order_ut.cpp
    kqp_native_index_ut.cpp
    kqp_native_optimizer_pragma_ut.cpp
    kqp_native_quoted_ut.cpp
    kqp_native_frontend_ut.cpp
    kqp_native_host_ut.cpp
)

# Keep source/stack locations for this large test binary without exceeding
# DWARF32 debug-section offsets. Production libraries keep their usual debug
# information; this setting applies only to the test translation units.
IF (OS_LINUX AND CLANG)
    CFLAGS(-gline-tables-only)
ENDIF()

PEERDIR(
    yql/essentials/minikql/comp_nodes
    yql/essentials/providers/common/mkql
    yql/essentials/udfs/common/string
    library/cpp/resource
    ydb/core/kqp/ut/common
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    ydb/library/benchmarks/queries/tpch
    ydb/library/plan2svg
    ydb/public/lib/ut_helpers
    ydb/library/yql/udfs/statistics_internal
    ydb/core/kqp/ut/olap/helpers
    ydb/core/statistics/ut_common
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/udfs/common/url_base
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

DATA (
    arcadia/ydb/core/kqp/ut/join/data
    arcadia/ydb/core/kqp/ut/rbo/data
)

RESOURCE(
    ydb/library/benchmarks/gen_queries/consts.yql consts.yql
    ydb/library/benchmarks/gen_queries/consts_decimal.yql consts_decimal.yql
)


IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
   -Dfstat=microsoft_native_fstat
   -Dstat=microsoft_native_stat
)
ENDIF()

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    effects
    native_boundaries
)
