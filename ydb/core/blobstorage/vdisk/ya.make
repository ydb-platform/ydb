LIBRARY()

SRCS(
    defs.h
    vdisk_actor.cpp
    vdisk_actor.h
    vdisk_services.h
)

PEERDIR(
    ydb/core/blobstorage/vdisk/anubis_osiris
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/defrag
    ydb/core/blobstorage/vdisk/huge
    ydb/core/blobstorage/vdisk/hulldb
    ydb/core/blobstorage/vdisk/hullop
    ydb/core/blobstorage/vdisk/ingress
    ydb/core/blobstorage/vdisk/localrecovery
    ydb/core/blobstorage/vdisk/protos
    ydb/core/blobstorage/vdisk/query
    ydb/core/blobstorage/vdisk/repl
    ydb/core/blobstorage/vdisk/scrub
    ydb/core/blobstorage/vdisk/skeleton
    ydb/core/blobstorage/vdisk/syncer
    ydb/core/blobstorage/vdisk/synclog
)

END()

RECURSE(
    anubis_osiris
    balance
    common
    defrag
    huge
    hulldb
    hullop
    ingress
    localrecovery
    query
    repl
    scrub
    skeleton
    syncer
    synclog
)
