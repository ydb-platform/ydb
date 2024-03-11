Y_BENCHMARK(jsonpath-benchmark)

PEERDIR(
    library/cpp/json
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/minikql/jsonpath
    ydb/library/yql/public/issue
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
