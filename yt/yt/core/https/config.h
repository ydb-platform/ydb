#pragma once

#include "public.h"

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/crypto/config.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

struct TServerCredentialsConfig
    : public NCrypto::TSslContextConfig
{
    TDuration UpdatePeriod;

    REGISTER_YSON_STRUCT(TServerCredentialsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public NHttp::TServerConfig
{
    TServerCredentialsConfigPtr Credentials;

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClientCredentialsConfig
    : public NCrypto::TSslContextConfig
{
    REGISTER_YSON_STRUCT(TClientCredentialsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClientConfig
    : public NHttp::TClientConfig
{
    // If missing then builtin certificate store is used.
    TClientCredentialsConfigPtr Credentials;

    REGISTER_YSON_STRUCT(TClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
