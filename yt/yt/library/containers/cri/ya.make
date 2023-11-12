LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/rpc/grpc
    yt/yt/contrib/cri-api
)

SRCS(
    cri_api.cpp
    cri_executor.cpp
    config.cpp
)

ADDINCL(
    ONE_LEVEL
    yt/yt/contrib/cri-api
)

END()
