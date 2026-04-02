GTEST()

SRCS(
    iam_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/json
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/iam
)

END()
