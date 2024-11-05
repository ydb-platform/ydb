LIBRARY()

SRCS(
    workload_factory.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/library/accessor
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_query
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_value
)

END()
