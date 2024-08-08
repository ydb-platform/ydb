UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/fresh)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb
)

SRCS(
    fresh_appendix_ut.cpp
    fresh_data_ut.cpp
    fresh_segment_ut.cpp
    snap_vec_ut.cpp
)

END()
