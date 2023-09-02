LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    authentication_manager.cpp
    batching_secret_vault_service.cpp
    blackbox_cookie_authenticator.cpp
    caching_secret_vault_service.cpp
    cookie_authenticator.cpp
    blackbox_service.cpp
    default_secret_vault_service.cpp
    dummy_secret_vault_service.cpp
    helpers.cpp
    cypress_cookie.cpp
    cypress_cookie_authenticator.cpp
    cypress_cookie_login.cpp
    cypress_cookie_manager.cpp
    cypress_cookie_store.cpp
    cypress_token_authenticator.cpp
    cypress_user_manager.cpp
    secret_vault_service.cpp
    ticket_authenticator.cpp
    token_authenticator.cpp
    oauth_cookie_authenticator.cpp
    oauth_token_authenticator.cpp
    oauth_service.cpp
)

PEERDIR(
    # For Cypress token authenticator and Cypress cookie authenticator.
    yt/yt/client

    yt/yt/core/https
    yt/yt/library/auth
    yt/yt/library/tvm/service
    library/cpp/string_utils/quote
    library/cpp/string_utils/url
)

END()

RECURSE_FOR_TESTS(
    unittests
)
