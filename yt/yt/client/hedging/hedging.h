#pragma once

#include "public.h"

#include "hedging_executor.h"
#include "penalty_provider.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/cache/public.h>

/*! HedgingClient is a wrapper for several YT-clients with ability
 *  to retry asynchronously the same request with different underlying-clients.
 *  HedgingClient implements IClient interface and supports methods that do not change state of data on YT.
 *  Currently supported methods: LookupRows, VersionedLookupRows, SelectRows, ExplainQuery,
 *  CreateTableReader, GetNode, ListNode, NodeExists, CreateFileReader
 *
 *  For initial configuration every YT-client needs an InitialPenalty value.
 *  This value is used to determine in which order YT-clients will be used.
 *
 *  MinInitialPenalty - minimal retry timeout value out of all YT-clients.
 *  EffectivePenalty - is a delay value for starting a request with a corresponding YT-client.
 *  For every client this value is calculated as: InitialPenalty - MinInitialPenalty.
 *
 *  If any of the clients responses with a success result: requests to other YT-clients are cancelled.
 *
 *  If any of the clients responses with an error: it's InitialPenalty is
 *  increased by BanPenalty value for the next BanDuration time interval.
 *  Both BanPenalty and BanDuration values are set in MultiClientCluster config.
 */

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

using NCache::IClientsCachePtr;

// from config.proto
class THedgingClientConfig;

////////////////////////////////////////////////////////////////////////////////

NApi::IClientPtr CreateHedgingClient(const THedgingExecutorPtr& hedgingExecutor);

//! Method for creating HedgingClient with given options.
NApi::IClientPtr CreateHedgingClient(const THedgingClientOptionsPtr& config);

//! Method for creating HedgingClient with given options and ability to use penalty updater policy.
//! Currently for experimental usage.
NApi::IClientPtr CreateHedgingClient(const THedgingClientOptionsPtr& config, const IPenaltyProviderPtr& penaltyProvider);

//! Method for creating HedgingClient with given rpc clients config and preinitialized clients.
NApi::IClientPtr CreateHedgingClient(const THedgingClientOptionsPtr& config, const IClientsCachePtr& clientsCache);

//! Method for creating HedgingClient with given rpc clients config, preinitialized clients and PenaltyProvider.
NApi::IClientPtr CreateHedgingClient(
    const THedgingClientOptionsPtr& config,
    const IClientsCachePtr& clientsCache,
    const IPenaltyProviderPtr& penaltyProvider);

// The following methods should be moved to `ads/bsyeti/libs/ytex/client`.

//! Method for creating HedgingClient options from given config and preinitialized clients.
THedgingClientOptionsPtr GetHedgingClientConfig(const THedgingClientConfig& config);

//! Method for creating HedgingClient with given rpc clients config.
NApi::IClientPtr CreateHedgingClient(const THedgingClientConfig& config);

//! Method for creating HedgingClient with given rpc clients config and preinitialized clients.
NApi::IClientPtr CreateHedgingClient(const THedgingClientConfig& config, const IClientsCachePtr& clientsCache);

//! Method for creating HedgingClient with given rpc clients config, preinitialized clients and PenaltyProvider.
NApi::IClientPtr CreateHedgingClient(const THedgingClientConfig& config, const IClientsCachePtr& clientsCache, const IPenaltyProviderPtr& penaltyProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
