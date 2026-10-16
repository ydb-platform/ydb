UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page)

REQUIREMENTS(cpu:1)
SRCS(
    mon_render_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
