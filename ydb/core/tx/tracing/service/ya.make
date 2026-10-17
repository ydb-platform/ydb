LIBRARY()

SRCS(
    global.cpp  # gn: into ydb/core/tx/tracing/usage
    actor.cpp  # gn: into ydb/core/tx/tracing/usage
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/tracing/usage
)

END()
