UNITTEST_FOR(ydb/library/yql/minikql/jsonpath)

TAG(ya:manual)

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
    ydb/library/binary_json
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/core/issue/protos
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
