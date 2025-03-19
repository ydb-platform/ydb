LIBRARY()

SRCS(
    params.cpp
    impl.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp/src/client/value
)

END()
