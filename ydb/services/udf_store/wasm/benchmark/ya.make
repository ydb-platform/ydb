Y_BENCHMARK()
YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/wasm/api
    ydb/library/wasm/engine
    ydb/services/udf_store/wasm
    yql/essentials/minikql
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()
