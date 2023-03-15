LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/protos
    ydb/core/protos
)

SRCS(
    defs.h
    fresh_appendix.cpp
    fresh_appendix.h
    fresh_data.cpp
    fresh_data.h
    fresh_datasnap.cpp
    fresh_datasnap.h
    fresh_segment.cpp
    fresh_segment.h
    fresh_segment_impl.h
    snap_vec.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
