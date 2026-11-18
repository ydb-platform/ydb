# Host integration and legacy-compatibility workloads share this runtime
# fixture. Direct operator unit tests also live here and call the importer
# without allowing a host fallback to hide a lowering failure.
UNITTEST_FOR(ydb/core/kqp)

# This executable's debug information exceeds DWARF32 relocation limits in
# relwithdebinfo builds. Keep stripping local to this test executable.
IF (OS_LINUX)
    LDFLAGS(-Wl,--strip-debug)
ENDIF()

FORK_SUBTESTS()
SPLIT_FACTOR(16)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_new_parser_fallback_ut.cpp
    kqp_optimizer_pragma_ut.cpp
    kqp_parameter_table_ut.cpp
    kqp_rbo_lowering_ut.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/kqp/ut/common
    ydb/core/kqp/host/new_parser/rbo
    ydb/library/benchmarks/queries/tpch
    ydb/library/yql/udfs/statistics_internal
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/langver
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/udfs/common/url_base
)

DATA(
    arcadia/ydb/core/kqp/ut/join/data
    arcadia/ydb/core/kqp/ut/rbo/data
)

RESOURCE(
    ydb/library/benchmarks/gen_queries/consts.yql consts.yql
)

YQL_LAST_ABI_VERSION()

END()
