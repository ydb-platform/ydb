UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/compstrat)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

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
