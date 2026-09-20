UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport)

SIZE(SMALL)

SRCS(
    ic_storage_transport_actor_ut.cpp
    transport_chaos_injector_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/util/actorsys_test
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
