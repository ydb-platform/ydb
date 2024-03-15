RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

OWNER(
    molotkov-and
    g:kikimr
)

SRCS(
    mvp.cpp
    oidc_client.cpp
    openid_connect.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/public/api/client/yc_private/oauth/v1
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
)
