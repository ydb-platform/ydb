G_BENCHMARK()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql/arrow
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    block_groupby.cpp
)

YQL_LAST_ABI_VERSION()

END()
