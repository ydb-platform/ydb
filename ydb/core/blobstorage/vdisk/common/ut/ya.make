UNITTEST_FOR(ydb/core/blobstorage/vdisk/common)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/groupinfo
    ydb/core/erasure
)

SRCS(
    circlebufresize_ut.cpp
    circlebufstream_ut.cpp
    circlebuf_ut.cpp
    memusage_ut.cpp
    vdisk_config_ut.cpp
    vdisk_lsnmngr_ut.cpp
    vdisk_outofspace_ut.cpp
    vdisk_pdisk_error_ut.cpp
    vdisk_syncneighbors_ut.cpp
)

END()
