#pragma once

#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/api/public.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

using NCache::IClientsCache;
using NCache::IClientsCachePtr;

DECLARE_REFCOUNTED_STRUCT(TCounter)
DECLARE_REFCOUNTED_STRUCT(TLagPenaltyProviderCounters)

// TODO(bulatman) Rename to THedgingClientConfig.
DECLARE_REFCOUNTED_STRUCT(THedgingClientOptions)

DECLARE_REFCOUNTED_STRUCT(IPenaltyProvider)

DECLARE_REFCOUNTED_CLASS(THedgingExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
