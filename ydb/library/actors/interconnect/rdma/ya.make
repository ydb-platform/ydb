LIBRARY()

SRCS(
    cq_actor.cpp
    ctx.cpp
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

RECURSE_FOR_TESTS(
    ut
)
