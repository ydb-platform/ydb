#pragma once

#include <yt/yt/client/cache/rpc.h>

namespace NYT::NClient::NHedging::NRpc {

using NCache::TConfig;
using NCache::TClustersConfig;
using NCache::ECompressionCodec;
using NCache::GetConnectionConfig;
using NCache::ExtractClusterAndProxyRole;
using NCache::SetClusterUrl;
using NCache::CreateClient;

} // namespace NYT::NClient::NHedging::NRpc
