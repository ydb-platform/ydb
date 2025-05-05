#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <util/generic/strbuf.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

//! Cache of clients per cluster.
struct IClientsCache
    : public virtual TRefCounted
{
    virtual NApi::IClientPtr GetClient(TStringBuf clusterUrl) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientsCache)

////////////////////////////////////////////////////////////////////////////////

//! Creates clients cache which explicitly given config. Server name is always overwritten with requested.
IClientsCachePtr CreateClientsCache(const TClientsCacheConfigPtr& config, const TClientsCacheAuthentificationOptionsPtr& options);

//! Creates clients cache which shares same options.
IClientsCachePtr CreateClientsCache(const TClientsCacheConfigPtr& config, const NApi::TClientOptions& defaultClientOptions);

//! Creates clients cache which shares same config (except server name).
//! Note: It also registers a connection in the cache for getting a client from the cache by |connectionConfig.ClusterName|.
IClientsCachePtr CreateClientsCache(
    const NApi::NRpcProxy::TConnectionConfigPtr& connectionConfig,
    const NApi::TClientOptions& options);

//! Shortcut to use client options from env.
IClientsCachePtr CreateClientsCache(const NApi::NRpcProxy::TConnectionConfigPtr& connectionConfig);

//! Shortcut to create cache with custom options and default config.
IClientsCachePtr CreateClientsCache(const NApi::TClientOptions& options);

//! Shortcut to create cache with default config.
IClientsCachePtr CreateClientsCache();

//! Helper function to get a cluster config by |clusterUrl|.
NApi::NRpcProxy::TConnectionConfigPtr GetConnectionConfig(const TClientsCacheConfigPtr& config, TStringBuf clusterUrl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
