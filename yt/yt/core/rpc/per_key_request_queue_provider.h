#pragma once

#include "public.h"

#include "request_queue_provider.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/syncmap/map.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPerKeyRequestQueueProvider
    : public TRequestQueueProviderBase
{
public:
    using TKey = T;
    using TReconfigurationCallback = TCallback<void(const TKey&, const TRequestQueuePtr&)>;
    using TKeyFromRequestHeaderCallback = TCallback<TKey(const NProto::TRequestHeader&)>;

    TPerKeyRequestQueueProvider(
        TKeyFromRequestHeaderCallback keyFromRequestHeader,
        TReconfigurationCallback reconfigurationCallback);

    // IRequestQueueProvider implementation.
    TRequestQueue* GetQueue(const NProto::TRequestHeader& header) override;
    void ConfigureQueue(TRequestQueue* queue, const TMethodConfigPtr& config) override;

    void ReconfigureQueue(const TKey& key);
    void ReconfigureAllQueues();

    void UpdateThrottlingEnabledFlags(bool enableWeightThrottling, bool enableBytesThrottling);
    void UpdateDefaultConfigs(const TRequestQueueThrottlerConfigs& configs);

protected:
    virtual TRequestQueuePtr CreateQueueForKey(const TKey& key) = 0;

    virtual bool IsReconfigurationPermitted(const TKey& key) const;

private:
    TRequestQueue* DoGetQueue(const TKey& key);
    std::pair<bool, bool> ReadThrottlingEnabledFlags();

    NConcurrency::TSyncMap<TKey, TRequestQueuePtr> RequestQueues_;

    NThreading::TAtomicObject<TRequestQueueThrottlerConfigs> DefaultConfigs_;

    TKeyFromRequestHeaderCallback KeyFromRequestHeader_;
    TReconfigurationCallback ReconfigurationCallback_;

    NThreading::TReaderWriterSpinLock ThrottlingEnabledFlagsSpinLock_;
    bool WeightThrottlingEnabled_ = true;
    bool BytesThrottlingEnabled_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TPerUserRequestQueueProvider
    : public TPerKeyRequestQueueProvider<std::string>
{
private:
    using TBase = TPerKeyRequestQueueProvider<std::string>;

public:
    TPerUserRequestQueueProvider(
        TReconfigurationCallback reconfigurationCallback = {},
        NProfiling::TProfiler throttlersProfiler = {});

private:
    const NProfiling::TProfiler ThrottlersProfiler_;

    TRequestQueuePtr CreateQueueForKey(const std::string& userName) override;
    bool IsReconfigurationPermitted(const std::string& userName) const override;

    static TKeyFromRequestHeaderCallback CreateKeyFromRequestHeaderCallback();
};

DEFINE_REFCOUNTED_TYPE(TPerUserRequestQueueProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define PER_KEY_REQUEST_QUEUE_PROVIDER_INL_H_
#include "per_key_request_queue_provider-inl.h"
#undef PER_KEY_REQUEST_QUEUE_PROVIDER_INL_H_
