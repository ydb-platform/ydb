FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/grpc_streaming
    ydb/library/grpc/server
    ydb/library/services
)

END()
