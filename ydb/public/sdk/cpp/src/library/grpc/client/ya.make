LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    grpc_client_low.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/containers/stack_vector
)

END()
