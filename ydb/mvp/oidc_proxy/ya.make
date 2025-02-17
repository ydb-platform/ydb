RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    mvp.cpp
    context.cpp
    oidc_client.cpp
    openid_connect.cpp
    oidc_settings.cpp
    oidc_cleanup_page.cpp
    oidc_impersonate_start_page_nebius.cpp
    oidc_impersonate_stop_page_nebius.cpp
    oidc_protected_page_handler.cpp
    oidc_protected_page_yandex.cpp
    oidc_protected_page_nebius.cpp
    oidc_protected_page.cpp
    oidc_session_create_handler.cpp
    oidc_session_create_yandex.cpp
    oidc_session_create_nebius.cpp
    oidc_session_create.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/public/api/client/yc_private/oauth/v1
    library/cpp/getopt
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
)
