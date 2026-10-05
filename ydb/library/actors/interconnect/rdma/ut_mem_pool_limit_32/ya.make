GTEST()
#TIMEOUT(3600)
IF (OS_LINUX AND SANITIZER_TYPE != "memory")
    IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TIMEOUT(3600)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
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
