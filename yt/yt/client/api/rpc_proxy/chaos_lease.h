#pragma once

#include <yt/yt/client/api/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::IPrerequisitePtr CreateChaosLease(
    IClientPtr client,
    NRpc::IChannelPtr channel,
    NChaosClient::TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    std::optional<TDuration> pingPeriod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
