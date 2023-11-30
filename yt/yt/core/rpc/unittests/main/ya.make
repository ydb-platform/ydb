GTEST(unittester-core-rpc)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (NOT OS_WINDOWS AND NOT ARCH_AARCH64)
    ALLOCATOR(YT)
ENDIF()

PROTO_NAMESPACE(yt)

SRCS(
    yt/yt/core/rpc/unittests/handle_channel_failure_ut.cpp
    yt/yt/core/rpc/unittests/roaming_channel_ut.cpp
    yt/yt/core/rpc/unittests/rpc_ut.cpp
    yt/yt/core/rpc/unittests/viable_peer_registry_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/rpc/grpc
    yt/yt/core/rpc/unittests/lib
    yt/yt/core/test_framework
    library/cpp/testing/common
)

SIZE(MEDIUM)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:exotic_platform)
ENDIF()

END()
