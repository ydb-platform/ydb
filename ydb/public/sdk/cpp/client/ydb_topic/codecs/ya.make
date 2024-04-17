LIBRARY()

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_topic/codecs/codecs.h)

SRCS(
    codecs.h
    codecs.cpp
)

PEERDIR(
    library/cpp/streams/zstd
    ydb/library/yql/public/issue/protos
    ydb/public/api/grpc/draft
    ydb/public/api/grpc
    ydb/public/api/protos
)

PROVIDES(topic_codecs)

END()
