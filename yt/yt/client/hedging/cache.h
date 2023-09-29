#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/cache/cache.h>

#include <util/generic/strbuf.h>


namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

using NCache::TConfig;
using NCache::TClustersConfig;
using NCache::CreateClientsCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
