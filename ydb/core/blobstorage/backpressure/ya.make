LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    ydb/library/actors/core
    library/cpp/containers/intrusive_rb_tree
    ydb/core/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/blobstorage/vdisk/common
    ydb/core/protos
)

SRCS(
    common.h
    defs.h
    event.cpp
    event.h
    queue.cpp
    queue.h
    queue_backpressure_client.cpp
    queue_backpressure_client.h
    queue_backpressure_common.h
    queue_backpressure_server.h
    unisched.cpp
    unisched.h
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_client
)
