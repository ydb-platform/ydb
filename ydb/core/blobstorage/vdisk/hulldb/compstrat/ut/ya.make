UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/compstrat)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb
    ydb/core/blobstorage/vdisk/hulldb/test
)

SRCS(
    hulldb_compstrat_ut.cpp
)

END()
