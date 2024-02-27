PROGRAM(config_proto_plugin)

PEERDIR(
    contrib/libs/protoc
    ydb/public/lib/protobuf
    ydb/core/config/protos
)

SRCS(
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
