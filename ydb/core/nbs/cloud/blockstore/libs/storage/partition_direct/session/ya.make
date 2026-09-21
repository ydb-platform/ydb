LIBRARY()

SRCS(
    partition_session_state.cpp
    events.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/protos
    ydb/library/actors/core
    library/cpp/threading/atomic_shared_ptr
)

END()

RECURSE_FOR_TESTS(ut)
