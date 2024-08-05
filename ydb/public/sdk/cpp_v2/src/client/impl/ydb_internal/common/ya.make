LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    parser.cpp
    getenv.cpp
    client_pid.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue
)

END()
