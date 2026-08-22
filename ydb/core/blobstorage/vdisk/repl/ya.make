LIBRARY()

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb/barriers
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/generic
    ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    ydb/core/protos
)

SRCS(
    blobstorage_hullrepljob.cpp
    blobstorage_hullrepljob.h
    blobstorage_hullreplwritesst.h
    blobstorage_replbroker.cpp
    blobstorage_replbroker.h
    blobstorage_repl.cpp
    blobstorage_replctx.h
    blobstorage_repl.h
    blobstorage_replproxy.cpp
    blobstorage_replproxy.h
    blobstorage_replrecoverymachine.h
    blobstorage_replmonhandler.cpp
    defs.h
    query_donor.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
