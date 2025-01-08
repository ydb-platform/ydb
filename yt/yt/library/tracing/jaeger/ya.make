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
    tracer.cpp
    GLOBAL configure_tracer.cpp
)

END()
