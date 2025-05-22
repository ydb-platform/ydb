LIBRARY()

SRCS(
    tier_info.cpp
    common.cpp
)

PEERDIR(
    ydb/core/formats/arrow/serializer
    ydb/core/tx/tiering/tier
)

END()
