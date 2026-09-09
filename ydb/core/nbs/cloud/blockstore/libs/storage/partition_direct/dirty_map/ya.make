LIBRARY()

GENERATE_ENUM_SERIALIZATION(ddisk_state.h)
GENERATE_ENUM_SERIALIZATION(dirty_map.h)
GENERATE_ENUM_SERIALIZATION(inflight_info.h)

SRCS(
    block_field_serializer.cpp
    ddisk_state.cpp
    dirty_map.cpp
    hints.cpp
    inflight_info.cpp
    range_locker.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
