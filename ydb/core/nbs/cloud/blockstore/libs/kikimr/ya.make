LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/protos
    ydb/core/nbs/cloud/storage/core/protos
)

END()
