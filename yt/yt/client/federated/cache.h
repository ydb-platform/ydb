#pragma once

#include "config.h"

#include <yt/yt/client/cache/cache.h>


namespace NYT::NClient::NFederated {

using NCache::IClientsCachePtr;
using NCache::TClustersConfig;
using NCache::TConfig;

////////////////////////////////////////////////////////////////////////////////

//! Creates clients cache for generic and federated clients.
//! Federated clients created with federatedConfig, and generic clients created with clustersConfig.
//! Which client to create decided by cluster url: if several clusters concatenated by clusterSeparator requested then
//! federated client it is, generic client otherwise.
//! For example, for "markov" generic client will be created, and for "seneca-sas+seneca-vla" federated one.
IClientsCachePtr CreateFederatedClientsCache(
    TConnectionConfigPtr federatedConfig,
    const TClustersConfig& clustersConfig,
    const NYT::NApi::TClientOptions& options,
    TString clusterSeparator = "+");

//! Creates clients cache for generic and federated clients.
//! Federated clients created with federatedConfig, and generic clients created with config.
//! Which client to create decided by cluster url: if several clusters concatenated by clusterSeparator requested then
//! federated client it is, generic client otherwise.
//! For example, for "markov" generic client will be created, and for "seneca-sas+seneca-vla" federated one.
IClientsCachePtr CreateFederatedClientsCache(
    TConnectionConfigPtr federatedConfig,
    const TConfig& config,
    const NYT::NApi::TClientOptions& options,
    TString clusterSeparator = "+");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
