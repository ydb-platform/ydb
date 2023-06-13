LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/library/yql/public/issue
    ydb/public/sdk/cpp/client/ydb_types/credentials
)

GENERATE_ENUM_SERIALIZATION(client.h)

END()
