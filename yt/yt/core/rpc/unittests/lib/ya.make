LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_service.cpp
    test_service.proto
    no_baggage_service.cpp
    no_baggage_service.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/core/rpc/grpc
    yt/yt/core/rpc/http
    yt/yt/core/https
    library/cpp/testing/gtest
)

END()
