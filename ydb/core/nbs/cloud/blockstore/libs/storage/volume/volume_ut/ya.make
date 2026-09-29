UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/volume)

FORK_SUBTESTS()

REQUIREMENTS(ram:32 cpu:2)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    volume_actor_ut.cpp
    volume_database_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/nbs/cloud/blockstore/bootstrap
    ydb/core/nbs/cloud/blockstore/config
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet
    ydb/core/nbs/cloud/blockstore/libs/storage/testlib
    ydb/core/nbs/nbs1_compat_api/cloud/storage/core/protos
    ydb/core/protos
    ydb/core/testlib
)

END()
