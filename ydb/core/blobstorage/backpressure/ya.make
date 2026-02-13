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
    ydb/core/retro_tracing_impl
)

SRCS(
    common.h
    defs.h
    event.cpp
    event.h
    load_based_timeout.cpp
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
