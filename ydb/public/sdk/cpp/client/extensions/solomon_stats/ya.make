LIBRARY()

SRCS(
    pull_client.cpp
    pull_connector.cpp
)

PEERDIR(
    library/cpp/monlib/encode/json
    library/cpp/monlib/metrics
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    ydb/public/sdk/cpp/client/ydb_extension
)

END()
