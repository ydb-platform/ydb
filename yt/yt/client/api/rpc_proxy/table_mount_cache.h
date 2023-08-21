#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NTabletClient::ITableMountCachePtr CreateTableMountCache(
    TTableMountCacheConfigPtr config,
    NRpc::IChannelPtr channel,
    const NLogging::TLogger& logger,
    TDuration timeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
