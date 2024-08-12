LIBRARY()

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_topic/include/codecs.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_topic/include/control_plane.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_topic/include/read_events.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_topic/include/write_events.h)

SRCS(
    client.h
    codecs.h
    control_plane.h
    counters.h
    errors.h
    events_common.h
    executor.h
    read_events.h
    read_session.h
    retry_policy.h
    write_events.h
    write_session.h
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos

    ydb/library/yql/public/issue/protos

    library/cpp/retry
    library/cpp/streams/zstd
)

END()
