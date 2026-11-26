LIBRARY()

SRCS(
    events.h
)

GENERATE_ENUM_SERIALIZATION(events.h)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tx
    ydb/public/api/protos
)

END()
