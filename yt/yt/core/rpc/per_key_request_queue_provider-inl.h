#ifndef PER_KEY_REQUEST_QUEUE_PROVIDER_INL_H_
#error "Direct inclusion of this file is not allowed, include per_key_request_queue_provider.h"
// For the sake of sane code completion.
#include "per_key_request_queue_provider.h"
#endif

#include "service_detail.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPerKeyRequestQueueProvider<T>::TPerKeyRequestQueueProvider(
    TKeyFromRequestHeaderCallback keyFromRequestHeader,
    TReconfigurationCallback reconfigurationCallback)
    : DefaultConfigs_(TRequestQueueThrottlerConfigs{
        InfiniteRequestThrottlerConfig,
        InfiniteRequestThrottlerConfig,
    })
    , KeyFromRequestHeader_(std::move(keyFromRequestHeader))
    , ReconfigurationCallback_(std::move(reconfigurationCallback))
{ }

template <class T>
TRequestQueue* TPerKeyRequestQueueProvider<T>::GetQueue(const NProto::TRequestHeader& header)
{
    return DoGetQueue(KeyFromRequestHeader_(header));
}

template <class T>
bool TPerKeyRequestQueueProvider<T>::IsReconfigurationPermitted(const T& /*key*/) const
{
    return true;
}

template <class T>
TRequestQueue* TPerKeyRequestQueueProvider<T>::DoGetQueue(const T& key)
{
    auto queue = RequestQueues_.FindOrInsert(key, [&] {
        auto queue = CreateQueueForKey(key);
        YT_VERIFY(key == std::any_cast<T>(queue->GetTag()));

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

        // NB: Not calling ReconfigurationCallback_ here because, for newly
        // created queues, ConfigureQueue is supposed to be called shortly.

        return queue;
    }).first;

    return queue->Get();
}

template <class T>
void TPerKeyRequestQueueProvider<T>::ConfigureQueue(
    TRequestQueue* queue,
    const TMethodConfigPtr& config)
{
    TRequestQueueProviderBase::ConfigureQueue(queue, config);

    if (ReconfigurationCallback_) {
        auto key = std::any_cast<T>(queue->GetTag());
        ReconfigurationCallback_(key, queue);
    }
}

template <class T>
std::pair<bool, bool> TPerKeyRequestQueueProvider<T>::ReadThrottlingEnabledFlags()
{
    auto guard = ReaderGuard(ThrottlingEnabledFlagsSpinLock_);
    return {
        WeightThrottlingEnabled_,
        BytesThrottlingEnabled_,
    };
}

template <class T>
void TPerKeyRequestQueueProvider<T>::UpdateThrottlingEnabledFlags(
    bool enableWeightThrottling,
    bool enableBytesThrottling)
{
    auto guard = WriterGuard(ThrottlingEnabledFlagsSpinLock_);
    WeightThrottlingEnabled_ = enableWeightThrottling;
    BytesThrottlingEnabled_ = enableBytesThrottling;
}

template <class T>
void TPerKeyRequestQueueProvider<T>::UpdateDefaultConfigs(
    const TRequestQueueThrottlerConfigs& configs)
{
    YT_ASSERT(configs.WeightThrottlerConfig && configs.BytesThrottlerConfig);
    DefaultConfigs_.Store(configs);
}

template <class T>
void TPerKeyRequestQueueProvider<T>::ReconfigureAllQueues()
{
    if (!ReconfigurationCallback_) {
        return;
    }

    RequestQueues_.Flush();
    RequestQueues_.IterateReadOnly([&] (const auto& key, const auto& queue) {
        ReconfigurationCallback_(key, queue);
    });
}

template <class T>
void TPerKeyRequestQueueProvider<T>::ReconfigureQueue(const T& key)
{
    if (!ReconfigurationCallback_) {
        return;
    }

    if (!IsReconfigurationPermitted(key)) {
        return;
    }

    if (auto* queue = RequestQueues_.Find(key)) {
        ReconfigurationCallback_(key, *queue);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
