LIBRARY()

SRCS(
    health.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/mon
    ydb/core/yq/libs/shared_resources
    ydb/public/sdk/cpp/client/ydb_discovery
)

YQL_LAST_ABI_VERSION()

END()
