GTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")

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

ENDIF()

END()
