LIBRARY()

GENERATE_ENUM_SERIALIZATION(dirty_map.h)
GENERATE_ENUM_SERIALIZATION(location.h)

SRCS(
    dirty_map.cpp
    inflight_info.cpp
    location.cpp
    range_locker.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
