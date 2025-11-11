LIBRARY()

SRCS(
    ctx.cpp
    ctx_open.cpp
    link_manager.cpp
    mem_pool.cpp
    rdma.cpp
)

PEERDIR(
    contrib/libs/ibdrv
    contrib/libs/protobuf
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/library/actors/util
)

END()

RECURSE(
    cq_actor
)

RECURSE_FOR_TESTS(
    ut
)
