UNITTEST_FOR(ydb/library/yql/udfs/common/topfreq/static)

SRCS(
    ../topfreq_udf_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
