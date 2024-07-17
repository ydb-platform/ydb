LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    raw_batch_request.cpp
    raw_requests.cpp
    rpc_parameters_serialization.cpp
)

PEERDIR(
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/http
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/interface/logging
    library/cpp/yson/node
)

END()

RECURSE_FOR_TESTS(
    ut
)
