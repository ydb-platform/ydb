UNITTEST_FOR(yql/essentials/minikql/jsonpath)



SRCS(
    common_ut.cpp
    examples_ut.cpp
    lax_ut.cpp
    strict_ut.cpp
    test_base.cpp
    lib_id_ut.cpp
)

PEERDIR(
    library/cpp/json
    yql/essentials/types/binary_json
    yql/essentials/minikql
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/dom
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/issue/protos
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
