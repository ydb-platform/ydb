#pragma once

#include "public.h"

#include <yt/yt/client/transaction_client/config.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

using TAlienRemoteTimestampProvidersMap = THashMap<NObjectClient::TCellTag, ITimestampProviderPtr>;

NRpc::IChannelPtr CreateTimestampProviderChannel(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory);

NRpc::IChannelPtr CreateTimestampProviderChannelFromAddresses(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    const std::vector<TString>& addresses);

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    TDuration batchPeriod);

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelPtr channel);

ITimestampProviderPtr CreateBatchingRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelPtr channel);

ITimestampProviderPtr CreateBatchingRemoteTimestampProvider(
    const TRemoteTimestampProviderConfigPtr& config,
    const NRpc::IChannelFactoryPtr& channelFactory);

TAlienRemoteTimestampProvidersMap CreateAlienTimestampProvidersMap(
    const std::vector<TAlienTimestampProviderConfigPtr>& configs,
    ITimestampProviderPtr nativeProvider,
    NObjectClient::TCellTag nativeProviderClockClusterTag,
    const NRpc::IChannelFactoryPtr& channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
