GTEST()

SRCS(
    iam_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/json
    library/cpp/testing/common
    ydb/public/sdk/cpp/src/client/iam
)

END()
