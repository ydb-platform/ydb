LIBRARY()

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_persqueue_public/include/control_plane.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_persqueue_public/include/read_events.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_persqueue_public/include/write_events.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_persqueue_public/include/write_session.h)

SRCS(
    aliases.h
    client.h
    control_plane.h
    read_events.h
    read_session.h
    write_events.h
    write_session.h
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos

    ydb/library/yql/public/issue/protos
)

END()
