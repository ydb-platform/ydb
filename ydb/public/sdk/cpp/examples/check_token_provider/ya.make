PROGRAM(check_token_provider)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
