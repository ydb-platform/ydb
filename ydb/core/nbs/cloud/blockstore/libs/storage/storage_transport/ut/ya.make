UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport)

SIZE(SMALL)

SRCS(
    transport_chaos_injector_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
