#pragma once

#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/api/public.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

using NCache::IClientsCache;
using NCache::IClientsCachePtr;

DECLARE_REFCOUNTED_STRUCT(TCounter)
DECLARE_REFCOUNTED_STRUCT(TLagPenaltyProviderCounters)

DECLARE_REFCOUNTED_STRUCT(TClientConfig)

DECLARE_REFCOUNTED_STRUCT(IPenaltyProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
