LIBRARY()

PEERDIR(
    library/cpp/json/writer
    library/cpp/monlib/service/pages
    library/cpp/threading/future
    ydb/core/base
    ydb/core/blobstorage/base
)

SRCS(
    defs.h
    mon_blob_range_page.cpp
    mon_blob_range_page.h
    mon_get_blob_page.cpp
    mon_get_blob_page.h
    mon_vdisk_stream.cpp
    mon_vdisk_stream.h
)

END()
