Y_BENCHMARK()

OWNER(
    g:kikimr
    g:yql
)

PEERDIR(
    library/cpp/json
    ydb/library/yql/minikql 
    ydb/library/yql/minikql/computation 
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/invoke_builtins 
    ydb/library/yql/minikql/jsonpath
    ydb/library/yql/public/issue
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()
 
SRCS(
    main.cpp
)

END() 
