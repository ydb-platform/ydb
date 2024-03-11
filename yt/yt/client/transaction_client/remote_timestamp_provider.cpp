#include "remote_timestamp_provider.h"
#include "batching_timestamp_provider.h"
#include "timestamp_provider_base.h"
#include "private.h"
#include "config.h"
#include "timestamp_service_proxy.h"

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTransactionClient {

using namespace NRpc;
using namespace NYTree;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateTimestampProviderChannel(
    TRemoteTimestampProviderConfigPtr config,
    IChannelFactoryPtr channelFactory)
{
    auto endpointDescription = TString("TimestampProvider");
    auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("timestamp_provider").Value(true)
        .EndMap());
    auto channel = CreateBalancingChannel(
        config,
        std::move(channelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes));
    channel = CreateRetryingChannel(
        config,
        std::move(channel));
    return channel;
}

IChannelPtr CreateTimestampProviderChannelFromAddresses(
    TRemoteTimestampProviderConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const std::vector<TString>& discoveredAddresses)
{
    auto channelConfig = CloneYsonStruct(config);
    if (!discoveredAddresses.empty()) {
        channelConfig->Addresses = discoveredAddresses;
    }
    return CreateTimestampProviderChannel(channelConfig, channelFactory);
}

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProvider
    : public TTimestampProviderBase
{
public:
    TRemoteTimestampProvider(
        IChannelPtr channel,
        TRemoteTimestampProviderConfigPtr config)
        : TTimestampProviderBase(config->LatestTimestampUpdatePeriod)
        , Config_(std::move(config))
        , Proxy_(std::move(channel))
    {
        Proxy_.SetDefaultTimeout(Config_->RpcTimeout);
    }

private:
    const TRemoteTimestampProviderConfigPtr Config_;

    TTimestampServiceProxy Proxy_;

    TFuture<TTimestamp> DoGenerateTimestamps(int count) override
    {
        auto req = Proxy_.GenerateTimestamps();
        req->SetResponseHeavy(true);
        req->set_count(count);
        return req->Invoke().Apply(BIND([] (const TTimestampServiceProxy::TRspGenerateTimestampsPtr& rsp) {
            return static_cast<TTimestamp>(rsp->timestamp());
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    IChannelPtr channel)
{
    return New<TRemoteTimestampProvider>(std::move(channel), std::move(config));
}

ITimestampProviderPtr CreateBatchingRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    IChannelPtr channel)
{
    auto underlying = CreateRemoteTimestampProvider(config, std::move(channel));
    return CreateBatchingTimestampProvider(
        std::move(underlying),
        config->BatchPeriod);
}

ITimestampProviderPtr CreateBatchingRemoteTimestampProvider(
    const TRemoteTimestampProviderConfigPtr& config,
    const IChannelFactoryPtr& channelFactory)
{
    return CreateBatchingRemoteTimestampProvider(
        config,
        CreateTimestampProviderChannel(config, channelFactory));
}

////////////////////////////////////////////////////////////////////////////////

TAlienRemoteTimestampProvidersMap CreateAlienTimestampProvidersMap(
    const std::vector<TAlienTimestampProviderConfigPtr>& configs,
    ITimestampProviderPtr nativeProvider,
    TCellTag nativeProviderClockClusterTag,
    const IChannelFactoryPtr& channelFactory)
{
    TAlienRemoteTimestampProvidersMap alienProvidersMap;

    if (nativeProviderClockClusterTag == InvalidCellTag) {
        return alienProvidersMap;
    }

    alienProvidersMap.reserve(configs.size() + 1);
    EmplaceOrCrash(alienProvidersMap, nativeProviderClockClusterTag, std::move(nativeProvider));

    for (const auto& foreignProviderConfig : configs) {
        auto alienClockCellTag = foreignProviderConfig->ClockClusterTag;

        if (alienProvidersMap.contains(alienClockCellTag)) {
            YT_LOG_ALERT("Duplicate entry for alien clock clusters (ClockClusterTag: %v)",
                alienClockCellTag);
            continue;
        }

        EmplaceOrCrash(
            alienProvidersMap,
            alienClockCellTag,
            CreateBatchingRemoteTimestampProvider(foreignProviderConfig->TimestampProvider, channelFactory));
    }

    return alienProvidersMap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

