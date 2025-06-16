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
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/udfs/common/url_base
    yql/essentials/udfs/common/json2
    yql/essentials/udfs/common/string
)

END()
