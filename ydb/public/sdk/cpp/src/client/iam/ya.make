LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/json
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/src/client/iam/common
)

END()
