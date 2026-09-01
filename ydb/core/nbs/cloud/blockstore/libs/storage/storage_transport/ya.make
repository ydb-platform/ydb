LIBRARY()

GENERATE_ENUM_SERIALIZATION(storage_transport.h)

SRCS(
    ddisk_helpers.cpp
    direct_session_registry.cpp
    ic_direct_storage_transport.cpp
    ic_storage_transport_actor.cpp
    ic_storage_transport_events.cpp
    ic_storage_transport.cpp
    session_reply_router.cpp
    storage_transport_mock.cpp
    storage_transport.cpp
    transport_chaos_injector.cpp
)

PEERDIR(
    library/cpp/threading/hot_swap

    ydb/core/mind/bscontroller

    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/kikimr
    ydb/core/nbs/cloud/blockstore/libs/storage/model

    ydb/core/nbs/cloud/storage/core/libs/common

    ydb/library/actors/interconnect
)

END()

RECURSE(
    testlib
)

RECURSE_FOR_TESTS(
    ut
)
