PROGRAM()

PEERDIR(
    contrib/libs/protoc
    ydb/public/lib/validation/helpers
    ydb/core/config/proto_options
)

SRCS(
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
    ut/protos
)
