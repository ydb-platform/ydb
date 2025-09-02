#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

struct THttpIOConfig
    : public virtual NYTree::TYsonStruct
{
    int ReadBufferSize;

    int MaxRedirectCount;

    bool IgnoreContinueResponses;

    TDuration ConnectionIdleTimeout;

    TDuration HeaderReadTimeout;
    TDuration BodyReadIdleTimeout;

    TDuration WriteIdleTimeout;

    REGISTER_YSON_STRUCT(THttpIOConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THttpIOConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public THttpIOConfig
{
    //! If zero then the port is chosen automatically.
    int Port;

    //! Limit for number of open TCP connections.
    int MaxSimultaneousConnections;
    int MaxBacklogSize;

    int BindRetryCount;
    TDuration BindRetryBackoff;

    bool EnableKeepAlive;

    std::optional<bool> CancelFiberOnConnectionClose;

    //! Disables Nagle's algorithm.
    bool NoDelay;

    //! This field is not accessible from config.
    bool IsHttps = false;

    //! Used for thread naming.
    //! CamelCase identifiers are preferred.
    //! This field is not accessible from config.
    TString ServerName = "Http";

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClientConfig
    : public THttpIOConfig
{
    int MaxIdleConnections;
    NNet::TDialerConfigPtr Dialer;
    bool OmitQuestionMarkForEmptyQuery;

    REGISTER_YSON_STRUCT(TClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRetryingClientConfig
    : public NYTree::TYsonStruct
{
    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration BackoffTimeout;
    int MaxAttemptCount;

    REGISTER_YSON_STRUCT(TRetryingClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRetryingClientConfig);

////////////////////////////////////////////////////////////////////////////////

struct TCorsConfig
    : public NYTree::TYsonStruct
{
    bool DisableCorsCheck;
    std::vector<TString> HostAllowList;
    std::vector<TString> HostSuffixAllowList;

    REGISTER_YSON_STRUCT(TCorsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCorsConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
