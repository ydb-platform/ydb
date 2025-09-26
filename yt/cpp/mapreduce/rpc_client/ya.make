LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    client_impl.cpp
    raw_batch_request.cpp
    raw_client.cpp
    rpc_parameters_serialization.cpp
    wrap_rpc_error.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/yt/client
)

END()
