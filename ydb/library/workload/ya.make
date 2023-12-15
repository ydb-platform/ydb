LIBRARY()

SRCS(
    stock_workload.cpp
    kv_workload.cpp
    workload_factory.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_table
)

END()
