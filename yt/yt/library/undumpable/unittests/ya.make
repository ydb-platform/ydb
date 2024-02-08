GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

SRCS(
    undumpable_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/undumpable
)

END()
