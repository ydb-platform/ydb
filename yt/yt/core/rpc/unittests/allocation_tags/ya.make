GTEST(core-rpc-allocation-tags)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

PROTO_NAMESPACE(yt)

SRCS(
    yt/yt/core/rpc/unittests/rpc_allocation_tags_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/ytprof
    yt/yt/library/profiling
    yt/yt/core
    yt/yt/core/rpc/grpc
    yt/yt/core/rpc/unittests/lib
    yt/yt/core/test_framework
    library/cpp/testing/common
)

SIZE(MEDIUM)

END()
