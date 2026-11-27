UNITTEST()

YQL_LAST_ABI_VERSION()

SRCS(
    compute_ut.cpp
    ../../../comp_nodes/ut/mkql_test_factory.cpp
)

PEERDIR(
    yql/essentials/minikql/async/compute
    yql/essentials/providers/common/mkql
    yql/essentials/core/type_ann
    yql/essentials/minikql/udf_value_test_support
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
)

END()
