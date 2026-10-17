PROGRAM(hnsw_search_workspaces_bench)

PEERDIR(
    ydb/core/tx/datashard/hnsw
    ydb/public/api/protos
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(main.cpp)

END()
