PROGRAM()

PEERDIR(
    contrib/libs/protoc
    ydb/public/api/protos/annotations
    ydb/public/lib/protobuf
)

SRCS(
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
