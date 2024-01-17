LIBRARY()

SRCS(
    GLOBAL stock_workload.cpp
    GLOBAL kv_workload.cpp
    workload_factory.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_table
)

END()
