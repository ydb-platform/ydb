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
    : public TRefCounted
{
    virtual NApi::IClientPtr GetClient(TStringBuf clusterUrl) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientsCache)

////////////////////////////////////////////////////////////////////////////////

//! Creates clients cache which explicitly given config. Server name is always overwritten with requested.
IClientsCachePtr CreateClientsCache(const TClustersConfig& config, const NApi::TClientOptions& options);

//! Creates clients cache which shares same config (except server name).
IClientsCachePtr CreateClientsCache(const TConfig& config, const NApi::TClientOptions& options);

//! Shortcut to use client options from env.
IClientsCachePtr CreateClientsCache(const TConfig& config);

//! Shortcut to create cache with custom options and proxy role.
IClientsCachePtr CreateClientsCache(const NApi::TClientOptions& options);

//! Shortcut to create cache with default config.
IClientsCachePtr CreateClientsCache();

//! Helper function to create one cluster config from cluster URL and clusters config.
TConfig MakeClusterConfig(const TClustersConfig& config, TStringBuf clusterUrl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
