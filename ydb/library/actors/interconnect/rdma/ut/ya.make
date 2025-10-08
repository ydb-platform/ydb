GTEST()

SRCS(
    allocator_ut.cpp
    ibv_ut.cpp
    utils.cpp
)

PEERDIR(
    contrib/libs/ibdrv
    ydb/library/actors/core
    ydb/library/actors/interconnect/rdma
)

END()
