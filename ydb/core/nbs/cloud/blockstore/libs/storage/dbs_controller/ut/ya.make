UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller)

SRCS(
    dbs_controller_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/testlib
    ydb/core/testlib/basics
    yql/essentials/sql/pg_dummy
)

END()
