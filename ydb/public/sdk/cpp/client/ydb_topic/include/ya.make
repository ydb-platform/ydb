LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

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
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
)

END()
