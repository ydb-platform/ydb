#pragma once

#include "private.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/transaction_client/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::ITimestampProviderPtr CreateTimestampProvider(
    NRpc::IChannelPtr channel,
    TDuration rpcTimeout,
    TDuration latestTimestampUpdatePeriod,
    NObjectClient::TCellTag clockClusterTag = NObjectClient::InvalidCellTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
