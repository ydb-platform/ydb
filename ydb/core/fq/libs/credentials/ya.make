LIBRARY()

SRCS(
    structured_token_credentials.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/yql/providers/common/token_accessor/client
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/iam_private
    yql/essentials/providers/common/structured_token
)

END()
