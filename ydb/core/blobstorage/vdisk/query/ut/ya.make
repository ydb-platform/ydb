UNITTEST_FOR(ydb/core/blobstorage/vdisk/query)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/vdisk/huge
    ydb/core/protos
)

SRCS(
    query_spacetracker_ut.cpp
    query_statdb_stream_ut.cpp
    query_statalgo_ut.cpp
    query_stat_yield_ut.cpp
)

END()
