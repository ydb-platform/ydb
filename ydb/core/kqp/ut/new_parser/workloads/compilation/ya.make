UNITTEST_FOR(ydb/core/kqp)

# Keep timing cases in one process and run them sequentially. Ordinary test
# runs return immediately unless ENABLE_PERF_COMPILATION is supplied.
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TIMEOUT(3600)
REQUIREMENTS(cpu:2)

SRCS(kqp_compilation_time_ut.cpp)

PEERDIR(
    ydb/library/actors/wilson/test_util
    ydb/core/kqp/ut/common
    ydb/library/yql/udfs/statistics_internal
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/langver
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/udfs/common/url_base
)

DATA(arcadia/ydb/core/kqp/ut/rbo/data)

YQL_LAST_ABI_VERSION()

END()
