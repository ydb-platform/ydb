UNITTEST_FOR(ydb/core/blobstorage/vdisk/common)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/groupinfo
    ydb/core/erasure
)

SRCS(
    blobstorage_cost_tracker_ut.cpp
    circlebufresize_ut.cpp
    circlebufstream_ut.cpp
    circlebuf_ut.cpp
    memusage_ut.cpp
    vdisk_config_ut.cpp
    vdisk_events_ut.cpp
    vdisk_histogram_latency_ut.cpp
    vdisk_lsnmngr_ut.cpp
    vdisk_outofspace_ut.cpp
    vdisk_pdisk_error_ut.cpp
    vdisk_syncneighbors_ut.cpp
)

END()
