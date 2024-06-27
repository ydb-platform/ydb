UNITTEST_FOR(ydb/core/blobstorage/vdisk/hulldb/base)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb
)

SRCS(
    blobstorage_blob_ut.cpp
    blobstorage_hullsatisfactionrank_ut.cpp
    blobstorage_hullstorageratio_ut.cpp
    hullbase_barrier_ut.cpp
    hullds_generic_it_ut.cpp
    hullds_heap_it_ut.cpp
)

END()
