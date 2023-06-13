PROGRAM()

PEERDIR(
    contrib/libs/protoc
    ydb/public/api/protos/annotations
)

SRCS(
    helpers.cpp
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
    ut/protos
)
