#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <util/generic/string.h>

namespace NMVP::NOIDC {

struct TOpenIdConnectSettings {
    static const inline TString YDB_OIDC_COOKIE = "ydb_oidc_cookie";
    static const inline TString SESSION_COOKIE = "session_cookie";
    static const inline TString IMPERSONATED_COOKIE = "impersonated_cookie";

    static const inline TString DEFAULT_CLIENT_ID = "yc.oauth.ydb-viewer";
    static const inline TString DEFAULT_AUTH_URL_PATH = "/oauth/authorize";
    static const inline TString DEFAULT_TOKEN_URL_PATH = "/oauth/token";
    static const inline TString DEFAULT_EXCHANGE_URL_PATH = "/oauth2/session/exchange";
    static const inline TString DEFAULT_IMPERSONATE_URL_PATH = "/oauth2/impersonation/impersonate";

    TString ClientId = DEFAULT_CLIENT_ID;
    TString SessionServiceEndpoint;
    TString SessionServiceTokenName;
    TString AuthorizationServerAddress;
    TString ClientSecret;
    std::vector<TString> AllowedProxyHosts;

    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TString AuthUrlPath = DEFAULT_AUTH_URL_PATH;
    TString TokenUrlPath = DEFAULT_TOKEN_URL_PATH;
    TString ExchangeUrlPath = DEFAULT_EXCHANGE_URL_PATH;
    TString ImpersonateUrlPath = DEFAULT_IMPERSONATE_URL_PATH;

    TString GetAuthorizationString() const;
    TString GetAuthEndpointURL() const;
    TString GetTokenEndpointURL() const;
    TString GetExchangeEndpointURL() const;
    TString GetImpersonateEndpointURL() const;
};

} // NMVP::NOIDC
