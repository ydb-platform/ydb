#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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

    static constexpr inline TDuration DEFAULT_REQUEST_TIMEOUT = TDuration::Seconds(120);

    static const TVector<TStringBuf> REQUEST_HEADERS_WHITE_LIST;
    static const TVector<TStringBuf> RESPONSE_HEADERS_WHITE_LIST;
    static constexpr TStringBuf WHOAMI_PATHS[] =  { "/viewer/json/whoami", "/viewer/whoami" };

    TString ClientId = DEFAULT_CLIENT_ID;
    TString SessionServiceEndpoint;
    TString SessionServiceTokenName;
    TString AuthorizationServerAddress;
    TString SecretName;
    TString ClientSecret;
    std::vector<TString> AllowedProxyHosts;
    TString WhoamiExtendedInfoEndpoint;
    TDuration DefaultRequestTimeout = DEFAULT_REQUEST_TIMEOUT;
    THashMap<TStringBuf, TDuration> RequestTimeoutsByPath;

    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TString AuthUrlPath = DEFAULT_AUTH_URL_PATH;
    TString TokenUrlPath = DEFAULT_TOKEN_URL_PATH;
    TString ExchangeUrlPath = DEFAULT_EXCHANGE_URL_PATH;
    TString ImpersonateUrlPath = DEFAULT_IMPERSONATE_URL_PATH;

    bool EnabledExtensionWhoami() const;
    void InitRequestTimeoutsByPath();

    TString GetAuthorizationString() const;
    TString GetAuthEndpointURL() const;
    TString GetTokenEndpointURL() const;
    TString GetExchangeEndpointURL() const;
    TString GetImpersonateEndpointURL() const;
};

} // NMVP::NOIDC
