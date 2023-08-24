#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <util/generic/strbuf.h>


namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

class TConfig;
class TClustersConfig;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NClient::NHedging::NRpc {

using NYT::NClient::NCache::TConfig;
using NYT::NClient::NCache::TClustersConfig;

////////////////////////////////////////////////////////////////////////////////

//! Cache of  clients per cluster.
class IClientsCache
    : public TRefCounted
{
public:
    virtual NApi::IClientPtr GetClient(TStringBuf clusterUrl) = 0;
};

DECLARE_REFCOUNTED_TYPE(IClientsCache)
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


//! Helper function to create one cluster config from cluster url and clusters config
TConfig MakeClusterConfig(const TClustersConfig& config, TStringBuf clusterUrl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
