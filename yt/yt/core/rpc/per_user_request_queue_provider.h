#pragma once

#include "public.h"

#include "request_queue_provider.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TPerUserRequestQueueProvider
    : public TRequestQueueProviderBase
{
public:
    using TReconfigurationCallback = std::function<void(const std::string, const TRequestQueuePtr&)>;

    explicit TPerUserRequestQueueProvider(
        TReconfigurationCallback reconfigurationCallback = {},
        NProfiling::TProfiler throttlerProfiler = {});

    // IRequestQueueProvider implementation.
    TRequestQueue* GetQueue(const NProto::TRequestHeader& header) override;
    void ConfigureQueue(TRequestQueue* queue, const TMethodConfigPtr& config) override;

    void ReconfigureUser(const std::string& userName);
    void ReconfigureAllUsers();

    void UpdateThrottlingEnabledFlags(bool enableWeightThrottling, bool enableBytesThrottling);
    void UpdateDefaultConfigs(const TRequestQueueThrottlerConfigs& configs);

private:
    TRequestQueue* DoGetQueue(const std::string& userName);
    std::pair<bool, bool> ReadThrottlingEnabledFlags();

    NConcurrency::TSyncMap<std::string, TRequestQueuePtr> RequestQueues_;

    TAtomicObject<TRequestQueueThrottlerConfigs> DefaultConfigs_;

    TReconfigurationCallback ReconfigurationCallback_;

    NThreading::TReaderWriterSpinLock ThrottlingEnabledFlagsSpinLock_;
    bool WeightThrottlingEnabled_ = false;
    bool BytesThrottlingEnabled_ = true;

    NProfiling::TProfiler ThrottlerProfiler_;
};

DECLARE_REFCOUNTED_CLASS(TPerUserRequestQueueProvider)
DEFINE_REFCOUNTED_TYPE(TPerUserRequestQueueProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
