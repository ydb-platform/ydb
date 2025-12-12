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
)

PEERDIR(
    ydb/library/actors/interconnect/rdma
    ydb/library/actors/interconnect/rdma/ut/utils
)

ENDIF()

END()
