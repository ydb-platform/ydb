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
    library/cpp/threading/future
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/library/yql/public/issue
)

GENERATE_ENUM_SERIALIZATION(client.h)

END()
