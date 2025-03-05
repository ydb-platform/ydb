LIBRARY()

SRCS(
    schema.cpp
    update.cpp
    validator.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/tiering/tier
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()
