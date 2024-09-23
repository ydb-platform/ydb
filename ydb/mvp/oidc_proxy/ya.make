RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    mvp.cpp
    oidc_client.cpp
    openid_connect.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/public/api/client/yc_private/oauth
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
)
