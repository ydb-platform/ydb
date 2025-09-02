#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <vector>

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

struct TFederationConfig
    : public virtual NYTree::TYsonStruct
{
    //! Bundle name which liveness should be checked on the background.
    std::optional<std::string> BundleName;

    //! How often cluster liveness should be checked on the background.
    TDuration ClusterHealthCheckPeriod;

    //! Checks Cypress root availability in liveness check.
    bool CheckCypressRoot;

    //! Maximum number of retry attempts to make.
    int ClusterRetryAttempts;

    //! For testing purposes only. Retry any error through a different cluster.
    bool RetryAnyError;

    REGISTER_YSON_STRUCT(TFederationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFederationConfig)

struct TConnectionConfig
    : public TFederationConfig
{
    //! The RPC connection config for participants clusters.
    std::vector<NApi::NRpcProxy::TConnectionConfigPtr> RpcProxyConnections;

    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
