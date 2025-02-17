LIBRARY()

SRCS(
    workload_factory.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/library/accessor
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/value
)

END()
