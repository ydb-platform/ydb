GTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    allocator_ut.cpp
    ibv_ut.cpp
    utils.cpp
    rdma_low_ut.cpp
)

PEERDIR(
    contrib/libs/ibdrv
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/rdma
    ydb/library/actors/interconnect/rdma/cq_actor
    ydb/library/actors/testlib
)

END()
