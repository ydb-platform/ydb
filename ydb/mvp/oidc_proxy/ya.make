RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    context.cpp
    extension.cpp
    extension_final.cpp
    extension_manager.cpp
    extension_whoami.cpp
    mvp.cpp
    oidc_cleanup_page.cpp
    oidc_client.cpp
    oidc_impersonate_start_page_nebius.cpp
    oidc_impersonate_stop_page_nebius.cpp
    oidc_protected_page.cpp
    oidc_protected_page_handler.cpp
    oidc_protected_page_nebius.cpp
    oidc_protected_page_yandex.cpp
    oidc_session_create.cpp
    oidc_session_create_handler.cpp
    oidc_session_create_nebius.cpp
    oidc_session_create_yandex.cpp
    oidc_settings.cpp
    openid_connect.cpp
)

PEERDIR(
    ydb/core/util
    ydb/mvp/core
    ydb/mvp/oidc_proxy/protos
    ydb/public/api/client/nc_private/iam/v1
    ydb/public/api/client/yc_private/oauth
    library/cpp/getopt
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
)
