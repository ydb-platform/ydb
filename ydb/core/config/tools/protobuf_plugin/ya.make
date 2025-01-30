PROGRAM(config_proto_plugin)

PEERDIR(
    contrib/libs/protoc
    library/cpp/protobuf/json
    ydb/public/lib/protobuf
    ydb/core/config/protos
    ydb/core/config/utils
)

SRCS(
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
