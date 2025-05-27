UNITTEST_FOR(ydb/core/blobstorage/vdisk/huge)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blobstorage/groupinfo
    ydb/core/erasure
)

SRCS(
    blobstorage_hullhugeheap_ctx_ut.cpp
    blobstorage_hullhugeheap_ut.cpp
    blobstorage_hullhuge_ut.cpp
    top_ut.cpp
)

END()
