UNITTEST_FOR(ydb/core/blobstorage/vdisk/query)

IF (WITH_VALGRIND)
    FORK_SUBTESTS()
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    FORK_SUBTESTS()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/blobstorage/vdisk/huge
    ydb/core/protos
)

SRCS(
    query_spacetracker_ut.cpp
)

END()
