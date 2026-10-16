UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/compstrat)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb
    ydb/core/blobstorage/vdisk/hulldb/test
    ydb/library/actors/testlib
)

SRCS(
    hulldb_compstrat_ut.cpp
)

END()
