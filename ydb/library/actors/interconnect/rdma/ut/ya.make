GTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")
    REQUIREMENTS(cpu:4)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
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
    ydb/library/actors/interconnect/address
    ydb/library/actors/interconnect/rdma
    ydb/library/actors/interconnect/rdma/cq_actor
    ydb/library/actors/interconnect/rdma/ut/utils
    ydb/library/actors/testlib
)

ENDIF()

END()
