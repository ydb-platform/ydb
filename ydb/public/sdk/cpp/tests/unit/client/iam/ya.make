UNITTEST()

SRCS(
    iam_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/json
    ydb/public/sdk/cpp/src/client/iam
)

END()
