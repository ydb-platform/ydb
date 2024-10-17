LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

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
    ydb/public/sdk/cpp/src/client/persqueue_public/include
)

END()
