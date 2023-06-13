LIBRARY()

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/generic
    ydb/core/protos
)

SRCS(
    barriers_chain.cpp
    barriers_chain.h
    barriers_essence.cpp
    barriers_essence.h
    barriers_public.cpp
    barriers_public.h
    barriers_tree.cpp
    barriers_tree.h
    defs.h
    hullds_cache_barrier.h
    hullds_gcessence_defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
