LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/common
    ydb/core/protos
    ydb/library/actors/core
)

END()
