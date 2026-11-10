LIBRARY()

SRCS(
    client.cpp
    scheme_workload.cpp
)

PEERDIR(
    library/cpp/time_provider
    ydb/library/workload/scheme
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
)

END()
