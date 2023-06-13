UNITTEST_FOR(ydb/library/yql/core/arrow_kernels/registry)

PEERDIR(
)

YQL_LAST_ABI_VERSION()

SRCS(
    registry_ut.cpp
)

PEERDIR(
    ydb/library/yql/core/arrow_kernels/request
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/udfs/common/url_base
)

END()
