#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/library/tvm/service/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TAuthCacheConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration CacheTtl;
    TDuration OptimisticCacheTtl;
    TDuration ErrorTtl;

    REGISTER_YSON_STRUCT(TAuthCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxServiceConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NHttps::TClientConfigPtr HttpClient;
    TString Host;
    int Port;
    bool Secure;
    TString BlackboxServiceId;

    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration BackoffTimeout;
    bool UseLowercaseLogin;

    REGISTER_YSON_STRUCT(TBlackboxServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TString Scope;
    bool EnableScopeCheck;
    bool GetUserTicket;

    REGISTER_YSON_STRUCT(TBlackboxTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTicketAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    THashSet<TString> Scopes;
    bool EnableScopeCheck;

    REGISTER_YSON_STRUCT(TBlackboxTicketAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTicketAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TAuthCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxTokenAuthenticatorConfig
    : public TBlackboxTokenAuthenticatorConfig
    , public TCachingTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingBlackboxTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NYPath::TYPath RootPath;
    TString Realm;

    bool Secure;

    REGISTER_YSON_STRUCT(TCypressTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCypressTokenAuthenticatorConfig
    : public TCachingTokenAuthenticatorConfig
    , public TCypressTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingCypressTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TOAuthTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TOAuthTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TOAuthTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingOAuthTokenAuthenticatorConfig
    : public TOAuthTokenAuthenticatorConfig
    , public TCachingTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingOAuthTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingOAuthTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

static const auto DefaultCsrfTokenTtl = TDuration::Days(7);

class TBlackboxCookieAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TString Domain;

    std::optional<TString> CsrfSecret;
    TDuration CsrfTokenTtl;

    bool GetUserTicket;

    REGISTER_YSON_STRUCT(TBlackboxCookieAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TOAuthCookieAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TOAuthCookieAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TOAuthCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TOAuthServiceConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NHttp::TRetryingClientConfigPtr RetryingClient;
    NHttps::TClientConfigPtr HttpClient;

    TString Host;
    int Port;
    bool Secure;

    TString AuthorizationHeaderPrefix;
    TString UserInfoEndpoint;
    TString UserInfoLoginField;
    std::optional<TString> UserInfoSubjectField;
    std::optional<TString> UserInfoErrorField;

    REGISTER_YSON_STRUCT(TOAuthServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOAuthServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressUserManagerConfig
    : public virtual NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TCypressUserManagerConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressUserManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCookieAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TAuthCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingCookieAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCypressUserManagerConfig
    : public TCypressUserManagerConfig
{
public:
    TAuthCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingCypressUserManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingCypressUserManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxCookieAuthenticatorConfig
    : public TBlackboxCookieAuthenticatorConfig
    , public TCachingCookieAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingBlackboxCookieAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingOAuthCookieAuthenticatorConfig
    : public TOAuthCookieAuthenticatorConfig
    , public TCachingCookieAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingOAuthCookieAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingOAuthCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TDefaultSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonStruct
{
public:
    TString Host;
    int Port;
    bool Secure;
    NHttps::TClientConfigPtr HttpClient;
    TDuration RequestTimeout;
    TString VaultServiceId;
    TString Consumer;

    REGISTER_YSON_STRUCT(TDefaultSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDefaultSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBatchingSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonStruct
{
public:
    TDuration BatchDelay;
    int MaxSubrequestsPerRequest;
    NConcurrency::TThroughputThrottlerConfigPtr RequestsThrottler;

    REGISTER_YSON_STRUCT(TBatchingSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingSecretVaultServiceConfig
    : public TAsyncExpiringCacheConfig
{
public:
    TAsyncExpiringCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressCookieStoreConfig
    : public NYTree::TYsonStruct
{
    //! Store will renew cookie list with this frequency.
    TDuration FullFetchPeriod;

    //! Errors are cached for this period of time.
    TDuration ErrorEvictionTime;

    REGISTER_YSON_STRUCT(TCypressCookieStoreConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressCookieStoreConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressCookieGeneratorConfig
    : public NYTree::TYsonStruct
{
    //! Used to form ExpiresAt parameter.
    TDuration CookieExpirationTimeout;

    //! If cookie will expire within this period,
    //! authenticator will try to renew it.
    TDuration CookieRenewalPeriod;

    //! Controls Secure parameter of a cookie.
    //! If true, cookie will be used by user only
    //! in https requests which prevents cookie
    //! stealing because of unsecured connection,
    //! so this field should be set to true in production
    //! environments.
    bool Secure;

    //! Controls HttpOnly parameter of a cookie.
    bool HttpOnly;

    //! Domain parameter of generated cookies.
    std::optional<TString> Domain;

    //! Path parameter of generated cookies.
    TString Path;

    //! If set and if cookie is generated via login page,
    //! will redirect user to this page.
    std::optional<TString> RedirectUrl;

    REGISTER_YSON_STRUCT(TCypressCookieGeneratorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressCookieGeneratorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressCookieManagerConfig
    : public NYTree::TYsonStruct
{
    TCypressCookieStoreConfigPtr CookieStore;
    TCypressCookieGeneratorConfigPtr CookieGenerator;
    TCachingBlackboxCookieAuthenticatorConfigPtr CookieAuthenticator;

    REGISTER_YSON_STRUCT(TCypressCookieManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressCookieManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationManagerConfig
    : public virtual NYT::NYTree::TYsonStruct
{
public:
    bool RequireAuthentication;
    TCachingBlackboxTokenAuthenticatorConfigPtr BlackboxTokenAuthenticator;
    TCachingBlackboxCookieAuthenticatorConfigPtr BlackboxCookieAuthenticator;
    TBlackboxServiceConfigPtr BlackboxService;
    TCachingCypressTokenAuthenticatorConfigPtr CypressTokenAuthenticator;
    TTvmServiceConfigPtr TvmService;
    TBlackboxTicketAuthenticatorConfigPtr BlackboxTicketAuthenticator;
    TCachingOAuthCookieAuthenticatorConfigPtr OAuthCookieAuthenticator;
    TCachingOAuthTokenAuthenticatorConfigPtr OAuthTokenAuthenticator;
    TOAuthServiceConfigPtr OAuthService;

    TCypressCookieManagerConfigPtr CypressCookieManager;
    TCachingCypressUserManagerConfigPtr CypressUserManager;

    TString GetCsrfSecret() const;

    TInstant GetCsrfTokenExpirationTime() const;

    REGISTER_YSON_STRUCT(TAuthenticationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthenticationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
