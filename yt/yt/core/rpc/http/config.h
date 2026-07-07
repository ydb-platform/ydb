#pragma once

#include "public.h"

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/https/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public NYT::NHttps::TServerConfig
{
    //! Number of threads in the poller serving the underlying HTTP server.
    int PollerThreadCount;

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClientConfig
    : public NYTree::TYsonStruct
{
    //! Whether channels speak HTTPs rather than plain HTTP.
    bool Secure;

    //! Client TLS credentials; only meaningful when #Secure is set.
    NHttps::TClientCredentialsConfigPtr Credentials;

    //! Number of threads in the poller shared by all channels of the factory.
    int PollerThreadCount;

    REGISTER_YSON_STRUCT(TClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
