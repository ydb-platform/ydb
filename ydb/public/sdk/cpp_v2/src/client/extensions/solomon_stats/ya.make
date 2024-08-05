LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    pull_client.cpp
    pull_connector.cpp
)

PEERDIR(
    library/cpp/monlib/encode/json
    library/cpp/monlib/metrics
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    ydb/public/sdk/cpp_v2/src/client/extension_common
)

END()
