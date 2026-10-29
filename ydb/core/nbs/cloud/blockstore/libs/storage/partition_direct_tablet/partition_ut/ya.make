UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet)

FORK_SUBTESTS()

IF (NOT SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:1)
ELSE()
    REQUIREMENTS(ram:32 cpu:2)
ENDIF()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    bsc_proxy_ut.cpp
    part_database_ut.cpp
    partition_direct_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/protos
    ydb/core/testlib
)

END()
