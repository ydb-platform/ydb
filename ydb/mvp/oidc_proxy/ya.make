RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    mvp.cpp
    context.cpp
    context_storage.cpp
    oidc_client.cpp
    openid_connect.cpp
    oidc_settings.cpp
    oidc_protected_page_handler.cpp
    oidc_protected_page_yandex.cpp
    oidc_protected_page_nebius.cpp
    oidc_protected_page.cpp
    oidc_session_create_handler.cpp
    oidc_session_create_yandex.cpp
    oidc_session_create_nebius.cpp
    oidc_session_create.cpp
    restore_context_handler.cpp
    context_storage_refresher.cpp
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
