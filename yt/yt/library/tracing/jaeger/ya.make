LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/library/tracing
    yt/yt/library/tvm/service
    yt/yt/core/rpc/grpc
)

SRCS(
    model.proto
    sampler.cpp
    GLOBAL tracer.cpp
)

END()
