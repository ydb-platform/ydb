PROGRAM(layout_bench)

SRCS(layout_bench.cpp)

PEERDIR(
    library/cpp/json
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/vdisk/ingress
)

END()
