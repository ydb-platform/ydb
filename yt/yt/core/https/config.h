#pragma once

#include "public.h"

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/crypto/config.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

class TServerCredentialsConfig
    : public NYTree::TYsonStruct
{
public:
    NCrypto::TPemBlobConfigPtr PrivateKey;
    NCrypto::TPemBlobConfigPtr CertChain;
    TDuration UpdatePeriod;

    REGISTER_YSON_STRUCT(TServerCredentialsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NHttp::TServerConfig
{
public:
    TServerCredentialsConfigPtr Credentials;

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClientCredentialsConfig
    : public NYTree::TYsonStruct
{
public:
    NCrypto::TPemBlobConfigPtr PrivateKey;
    NCrypto::TPemBlobConfigPtr CertChain;

    REGISTER_YSON_STRUCT(TClientCredentialsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
    : public NHttp::TClientConfig
{
public:
    // If missing then builtin certificate store is used.
    TClientCredentialsConfigPtr Credentials;

    REGISTER_YSON_STRUCT(TClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
