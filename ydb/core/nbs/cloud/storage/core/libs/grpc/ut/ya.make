UNITTEST_FOR(ydb/core/nbs/cloud/storage/core/libs/grpc)

SRCDIR(ydb/core/nbs/cloud/storage/core/libs/grpc)

SRCS(
    executor_ut.cpp
    init_ut.cpp
    tls_certificate_provider_ut.cpp
    tls_utils_ut.cpp
    utils_ut.cpp
)

ADDINCL(
    contrib/libs/grpc
)

RESOURCE(
    certs/ca.crt        grpc/ut/certs/ca.crt
    certs/server1.crt   grpc/ut/certs/server1.crt
    certs/server1.key   grpc/ut/certs/server1.key
    certs/server2.crt   grpc/ut/certs/server2.crt
    certs/server2.key   grpc/ut/certs/server2.key
    certs/server3.crt   grpc/ut/certs/server3.crt
    certs/server3.key   grpc/ut/certs/server3.key
)

PEERDIR(
    library/cpp/resource
    library/cpp/testing/common
    library/cpp/testing/gmock_in_unittest
)

END()
