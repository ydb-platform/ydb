LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/actors
    ydb/core/nbs/cloud/storage/core/libs/common

    ydb/library/actors/core

    ydb/core/protos
)

END()
