LIBRARY()

GENERATE_ENUM_SERIALIZATION(request.h)

SRCS(
    aligned_device_handler.cpp
    blocks_info.cpp
    context.cpp
    device_handler.cpp
    durable_wrapper.cpp
    overlapped_requests_guard_wrapper.cpp
    request.cpp
    split_requests_wrapper.cpp
    storage_gate.cpp
    storage.cpp
    trace_service_gate.cpp
    unaligned_device_handler.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/public/api/protos
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/libs/coroutine
    ydb/core/nbs/cloud/storage/core/libs/diagnostics
    ydb/library/actors/wilson

    library/cpp/threading/hot_swap
)

END()

RECURSE_FOR_TESTS(ut)
