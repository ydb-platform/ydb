#include "per_user_request_queue_provider.h"

#include "service_detail.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

static const auto InfiniteRequestThrottlerConfig = New<NConcurrency::TThroughputThrottlerConfig>();

////////////////////////////////////////////////////////////////////////////////

TPerUserRequestQueueProvider::TPerUserRequestQueueProvider(
    TReconfigurationCallback reconfigurationCallback,
    NProfiling::TProfiler throttlerProfiler)
    : DefaultConfigs_(TRequestQueueThrottlerConfigs{
        InfiniteRequestThrottlerConfig,
        InfiniteRequestThrottlerConfig
    })
    , ReconfigurationCallback_(std::move(reconfigurationCallback))
    , ThrottlerProfiler_(std::move(throttlerProfiler))
{
    DoGetQueue(RootUserName);
}

TRequestQueue* TPerUserRequestQueueProvider::GetQueue(const NProto::TRequestHeader& header)
{
    auto userName = header.has_user() ? ::NYT::FromProto<TString>(header.user()) : RootUserName;
    return DoGetQueue(userName);
}

TRequestQueue* TPerUserRequestQueueProvider::DoGetQueue(const std::string& userName)
{
    auto queue = RequestQueues_.FindOrInsert(userName, [&] {
        auto queue = CreateRequestQueue(userName, ThrottlerProfiler_);

        // It doesn't matter if configs are outdated since we will reconfigure them
        // again in the automaton thread.
        auto configs = DefaultConfigs_.Load();

        auto [weightThrottlingEnabled, bytesThrottlingEnabled] =
            ReadThrottlingEnabledFlags();

        queue->ConfigureWeightThrottler(weightThrottlingEnabled
            ? configs.WeightThrottlerConfig
            : nullptr);
        queue->ConfigureBytesThrottler(bytesThrottlingEnabled
            ? configs.BytesThrottlerConfig
            : nullptr);

        // NB: not calling ReconfigurationCallback_ here because, for newly
        // created queues, ConfigureQueue is supposed to be called shortly.

        return queue;
    }).first;

    return queue->Get();
}

void TPerUserRequestQueueProvider::ConfigureQueue(
    TRequestQueue* queue,
    const TMethodConfigPtr& config)
{
    TRequestQueueProviderBase::ConfigureQueue(queue, config);

    if (ReconfigurationCallback_) {
        ReconfigurationCallback_(queue->GetName(), queue);
    }
}

std::pair<bool, bool> TPerUserRequestQueueProvider::ReadThrottlingEnabledFlags()
{
    std::pair<bool, bool> result;

    auto guard = ReaderGuard(ThrottlingEnabledFlagsSpinLock_);
    result.first = WeightThrottlingEnabled_;
    result.second = BytesThrottlingEnabled_;

    return result;
}

void TPerUserRequestQueueProvider::UpdateThrottlingEnabledFlags(
    bool enableWeightThrottling,
    bool enableBytesThrottling)
{
    auto guard = WriterGuard(ThrottlingEnabledFlagsSpinLock_);
    WeightThrottlingEnabled_ = enableWeightThrottling;
    BytesThrottlingEnabled_ = enableBytesThrottling;
}

void TPerUserRequestQueueProvider::UpdateDefaultConfigs(
    const TRequestQueueThrottlerConfigs& configs)
{
    YT_ASSERT(configs.WeightThrottlerConfig && configs.BytesThrottlerConfig);
    DefaultConfigs_.Store(configs);
}

void TPerUserRequestQueueProvider::ReconfigureAllUsers()
{
    if (!ReconfigurationCallback_) {
        return;
    }

    RequestQueues_.Flush();
    RequestQueues_.IterateReadOnly([&] (const auto& userName, const auto& queue) {
        ReconfigurationCallback_(userName, queue);
    });
}

void TPerUserRequestQueueProvider::ReconfigureUser(const std::string& userName)
{
    if (!ReconfigurationCallback_) {
        return;
    }

    // Extra layer of defense from throttling root and spelling doom.
    if (userName == RootUserName) {
        return;
    }

    if (auto* queue = RequestQueues_.Find(userName)) {
        ReconfigurationCallback_(userName, *queue);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
