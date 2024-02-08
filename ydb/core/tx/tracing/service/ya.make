LIBRARY()

SRCS(
    global.cpp
    actor.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/tracing/usage
)

END()
