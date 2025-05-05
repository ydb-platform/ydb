LIBRARY(sdk-library-grpc-client-v3)

SRCS(
    grpc_client_low.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/containers/stack_vector
)

END()
