#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/auth/authentication_options.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

struct TClientsCacheConfig
    : public virtual NYTree::TYsonStruct
{
    NApi::NRpcProxy::TConnectionConfigPtr DefaultConfig;

    THashMap<TString, NApi::NRpcProxy::TConnectionConfigPtr> ClusterConfigs;

    REGISTER_YSON_STRUCT(TClientsCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClientsCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClientsCacheAuthentificationOptions final
{
    // Get options with `DefaultOptions` filled from env.
    static TClientsCacheAuthentificationOptionsPtr GetFromEnvStatic();

    NAuth::TAuthenticationOptions DefaultOptions;
    THashMap<TString, NAuth::TAuthenticationOptions> ClusterOptions;
};

DEFINE_REFCOUNTED_TYPE(TClientsCacheAuthentificationOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
