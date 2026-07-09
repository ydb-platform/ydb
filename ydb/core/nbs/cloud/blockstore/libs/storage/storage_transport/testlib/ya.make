LIBRARY()

SRCS(
    ddisk_stub_actor.cpp
    ic_storage_transport_test_adapter.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/mind/bscontroller
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport
    ydb/core/testlib
    ydb/library/actors/core
)

END()
