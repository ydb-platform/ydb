LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    public.cpp
    dispatcher.cpp
    server.cpp
    helpers.cpp
    channel.cpp
    proto/grpc.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/core/crypto
    contrib/libs/grpc
)

ADDINCL(
    contrib/libs/grpc  # Needed for `grpc_core::Executor::SetThreadsLimit`.
)

END()
