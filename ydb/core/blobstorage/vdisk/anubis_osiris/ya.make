LIBRARY()

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/vdisk/hulldb/barriers
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/generic
    ydb/core/protos
)

SRCS(
    blobstorage_anubis.cpp
    blobstorage_anubis_algo.cpp
    blobstorage_anubis_osiris.cpp
    blobstorage_anubisfinder.cpp
    blobstorage_anubisproxy.cpp
    blobstorage_anubisrunner.cpp
    blobstorage_osiris.cpp
    defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
