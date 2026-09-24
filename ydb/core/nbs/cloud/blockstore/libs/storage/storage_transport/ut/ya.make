UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport)

SIZE(SMALL)

SRCS(
    ic_direct_storage_transport_ut.cpp
    ic_storage_transport_actor_ut.cpp
    transport_chaos_injector_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib
    ydb/core/util/actorsys_test
    yql/essentials/sql/pg_dummy
)

END()
