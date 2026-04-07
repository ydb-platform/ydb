LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_keys.cpp
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
    yt/yt/core/test_framework
    library/cpp/testing/gtest
)

END()
