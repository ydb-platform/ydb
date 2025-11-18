GTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    port_manager.cpp
    rdma_xdc_ut.cpp    
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/ut/protos
    ydb/library/actors/interconnect/ut/lib
    ydb/library/actors/interconnect/rdma/ut/utils
    ydb/library/actors/testlib
)

ENDIF()

END()
