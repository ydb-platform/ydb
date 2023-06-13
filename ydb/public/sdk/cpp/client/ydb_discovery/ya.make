LIBRARY()

SRCS(
    discovery.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
)

END()
