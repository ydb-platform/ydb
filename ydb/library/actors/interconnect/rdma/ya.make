LIBRARY()

SRCS(
    ctx.cpp
    ctx_open.cpp
    link_manager.cpp
    mem_pool.cpp
)

PEERDIR(
    contrib/libs/ibdrv
    contrib/libs/protobuf
    ydb/library/actors/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
