#pragma once

#include "config.h"

#include <yt/yt/client/cache/cache.h>


namespace NYT::NClient::NFederated {

using NCache::IClientsCachePtr;
using NCache::TClustersConfig;
using NCache::TConfig;

////////////////////////////////////////////////////////////////////////////////

//! Creates clients cache which explicitly given federation and clusters config.
//! Server name is always overwritten with requested.
//! Creates FederatedClient when clusters are enumerated with separator(default is '+').
//! For example: seneca-sas+seneca-vla.
IClientsCachePtr CreateFederatedClientsCache(
    TFederationConfigPtr federationConfig,
    const TClustersConfig& config,
    const NYT::NApi::TClientOptions& options,
    TString clusterSeparator = "+");

//! Shortcut to create cache with default federation config.
IClientsCachePtr CreateFederatedClientsCache(
    const TClustersConfig& config,
    const NYT::NApi::TClientOptions& options,
    TString chaosBundleName,
    TString clusterSeparator = "+");

//! Creates clients cache which shares same config (except server name).
//! Shortcut to create cache with default federation config.
IClientsCachePtr CreateFederatedClientsCache(
    const TConfig& config,
    const NYT::NApi::TClientOptions& options,
    TString chaosBundleName,
    TString clusterSeparator = "+");

//! Creates clients cache which shares same config (except server name).
//! Shortcut to create cache with default federation config.
//! Shortcut to use client options from env.
IClientsCachePtr CreateFederatedClientsCache(
    const TConfig& config,
    TString chaosBundleName,
    TString clusterSeparator = "+");

//! Shortcut to create cache with default federation config.
//! Shortcut to use client options from env.
//! Shortcut to create cache with default clusters config.
IClientsCachePtr CreateFederatedClientsCache(
    TString chaosBundleName,
    TString clusterSeparator = "+");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
