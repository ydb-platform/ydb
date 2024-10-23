PROGRAM()

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/protobuf/util
    library/cpp/threading/future
    ydb/library/yql/utils
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/yson_value
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/common
)

SRCS(
    main.cpp
)

END()
