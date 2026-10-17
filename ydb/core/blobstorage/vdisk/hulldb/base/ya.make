LIBRARY()

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk/protos
)

SRCS(
    blobstorage_blob.cpp  # gn: base_types
    blobstorage_blob.h
    blobstorage_hulldefs.cpp
    blobstorage_hulldefs.h
    blobstorage_hullsatisfactionrank.cpp
    blobstorage_hullsatisfactionrank.h
    blobstorage_hullstorageratio.h
    defs.h
    hullbase_barrier.cpp  # gn: base_types
    hullbase_barrier.h
    hullbase_block.h
    hullbase_data_parts.h
    hullbase_logoblob.h
    hullbase_rec.h
    hullds_arena.h
    hullds_generic_it.h
    hullds_heap_it.h
    hullds_glue.h
    hullds_settings.h
)
# gn: base_types headers blobstorage_blob.h hullbase_barrier.h hullbase_data_parts.h

END()

RECURSE_FOR_TESTS(
    ut
)
