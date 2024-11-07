UNITTEST_FOR(yql/essentials/core/arrow_kernels/registry)

PEERDIR(
)

YQL_LAST_ABI_VERSION()

SRCS(
    registry_ut.cpp
)

PEERDIR(
    yql/essentials/core/arrow_kernels/request
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/udfs/common/url_base
    contrib/ydb/library/yql/udfs/common/json2
)

END()
