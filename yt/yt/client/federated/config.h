#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <vector>

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

class TFederationConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Bundle name which liveness should be checked on the background.
    std::optional<TString> BundleName;

    //! How often cluster liveness should be checked on the background.
    TDuration ClusterHealthCheckPeriod;

    //! Maximum number of retry attempts to make.
    int ClusterRetryAttempts;

    //! For testing purposes only. Retry any error through a different cluster.
    bool RetryAnyError;

    REGISTER_YSON_STRUCT(TFederationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFederationConfig)

class TConnectionConfig
    : public TFederationConfig
{
public:
    //! The RPC connection config for participants clusters.
    std::vector<NApi::NRpcProxy::TConnectionConfigPtr> RpcProxyConnections;

    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
