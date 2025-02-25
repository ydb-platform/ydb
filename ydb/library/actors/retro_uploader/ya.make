LIBRARY()

SRCS(
    retro_uploader.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/library/actors/retro_uploader/protos
    ydb/library/actors/wilson
    ydb/library/retro_tracing
)

END()
