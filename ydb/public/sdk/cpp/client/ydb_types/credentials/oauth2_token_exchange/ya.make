LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    credentials.h
    from_file.h
    jwt_token_source.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/credentials/oauth2_token_exchange
)

END()
