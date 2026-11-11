LIBRARY()

SRCS(
    events.cpp
)

GENERATE_ENUM_SERIALIZATION(events.h)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/common
    ydb/core/protos
    ydb/library/actors/core
)

END()
