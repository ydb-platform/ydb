LIBRARY()

OWNER(
    g:kikimr
    g:logbroker
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_topic/topic.h)

SRCS(
    topic.h
    proto_accessor.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/public/sdk/cpp/client/ydb_topic/impl
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_driver
)

END()
