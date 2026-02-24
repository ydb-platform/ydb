UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct)

FORK_SUBTESTS()

REQUIREMENTS(ram:32 cpu:2)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()


SRCS(
    partition_direct_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/protos
    ydb/core/testlib
)

END()
