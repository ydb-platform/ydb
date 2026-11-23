UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/volume)

SRCS(
    volume_actor_ut.cpp
    volume_database_ut.cpp
    volume_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/nbs/cloud/blockstore/libs/storage/testlib
    ydb/core/protos
    ydb/core/testlib
    ydb/core/testlib/basics
    yql/essentials/sql/pg_dummy
)

END()
