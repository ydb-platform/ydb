LIBRARY()

SRCS(
    tier_info.cpp
    common.cpp
)

PEERDIR(
    yql/essentials/types/dynumber
    ydb/core/formats/arrow/serializer
    ydb/core/tx/tiering/tier
)

END()

RECURSE_FOR_TESTS(
    ut
)
