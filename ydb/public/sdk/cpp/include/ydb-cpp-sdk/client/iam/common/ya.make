LIBRARY(client-iam-common-include)

SRCS(
    types.h
)

PEERDIR(
<<<<<<< HEAD
=======
    contrib/libs/grpc
    ydb/public/sdk/cpp/src/library/issue
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506))
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/library/time
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/status
)

END()
