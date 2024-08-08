UNITTEST_FOR(ydb/library/yql/udfs/common/stat/static)

SRCS(
    ../stat_udf_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

TIMEOUT(300)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

END()
