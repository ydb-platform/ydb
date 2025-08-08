LIBRARY()

SRCS(
    discovery.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h)

PEERDIR(
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
)

END()
