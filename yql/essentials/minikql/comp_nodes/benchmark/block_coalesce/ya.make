G_BENCHMARK()

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/codegen/no_llvm
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/core/arrow_kernels/request
    yql/essentials/core/arrow_kernels/registry
)

YQL_LAST_ABI_VERSION()

SRCS(
    ../../ut/mkql_test_factory.cpp
    bench.cpp
)

END()
