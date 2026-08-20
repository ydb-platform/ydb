PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/types/credentials/oidc
)

END()
