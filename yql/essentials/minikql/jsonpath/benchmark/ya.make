Y_BENCHMARK(jsonpath-benchmark)

PEERDIR(
    library/cpp/json
    yql/essentials/minikql/dom
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/jsonpath
    yql/essentials/public/issue
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
