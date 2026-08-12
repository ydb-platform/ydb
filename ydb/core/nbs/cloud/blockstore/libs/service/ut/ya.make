UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/service)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    blocks_info_ut.cpp
    device_handler_ut.cpp
    durable_wrapper_ut.cpp
    overlapped_requests_guard_wrapper_ut.cpp
    split_requests_wrapper_ut.cpp
    storage_gate_ut.cpp
    trace_service_gate_ut.cpp
)

PEERDIR(
    ydb/library/actors/wilson
)

END()
