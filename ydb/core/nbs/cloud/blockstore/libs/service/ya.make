LIBRARY()

GENERATE_ENUM_SERIALIZATION(request.h)

SRCS(
    aligned_device_handler.cpp
    blocks_info.cpp
    context.cpp
    device_handler.cpp
    overlapped_requests_guard_wrapper.cpp
    request.cpp
    split_requests_wrapper.cpp
    storage.cpp
    unaligned_device_handler.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/public/api/protos
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/libs/coroutine
    ydb/library/actors/wilson
)

END()

RECURSE_FOR_TESTS(ut)
