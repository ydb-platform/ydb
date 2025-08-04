GTEST()


IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    allocator_ut.cpp
    ibv_ut.cpp
    rdma_low_ut.cpp
)

PEERDIR(
    ydb/library/actors/interconnect/rdma/ibdrv
    ydb/library/actors/interconnect/rdma
    ydb/library/actors/interconnect
    ydb/library/actors/testlib
)

END()
