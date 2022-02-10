LIBRARY()

OWNER(
    galaxycrab
    g:yq
    g:yql
)

SRCS(
    client.cpp
)

PEERDIR(
    library/cpp/grpc/client
    library/cpp/threading/future
    logbroker/public/api/grpc
    logbroker/public/api/protos
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/library/yql/providers/pq/cm_client/interface
    ydb/library/yql/public/issue
)

END()
