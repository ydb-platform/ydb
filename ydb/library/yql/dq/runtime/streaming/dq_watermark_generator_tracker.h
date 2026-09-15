#pragma once

#include "dq_watermark_tracker_impl.h"
#include "partition_key.h"

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

#include <functional>

namespace NYql::NDq {

class TDqWatermarkGeneratorTracker {
public:
    using TNotifyHandler = std::function<void(TInstant)>;

    TDqWatermarkGeneratorTracker(
        const TString& logPrefix,
        const ::NMonitoring::TDynamicCounterPtr& counters = {}
    );

    void SetSettings(
        TDuration granularity,
        bool idlePartitionsEnabled,
        TDuration lateArrivalDelay,
        TDuration idleTimeout
    );

    [[nodiscard]] const TString& GetLogPrefix() const;
    void SetLogPrefix(const TString& logPrefix);

    void SetNotifyHandler(TNotifyHandler notifyHandler);

    bool RegisterPartition(const TPartitionKey& partitionKey, TInstant systemTime);

    bool ProcessIdlenessCheck(TInstant systemTime);

    TMaybe<TInstant> NotifyNewPartitionTime(
        const TPartitionKey& partitionKey,
        TInstant partitionTime,
        TInstant systemTime
    );

    TMaybe<TInstant> HandleIdleness(TInstant systemTime);

private:
    void PrepareIdlenessCheck(TInstant systemTime);

    TInstant ToDiscreteTime(TInstant time) const;
    TInstant ToNextDiscreteTime(TInstant time) const;

private:
    TDuration Granularity_;
    bool IdlePartitionsEnabled_;
    TDuration LateArrivalDelay_;
    TDuration IdleTimeout_;

    TDqWatermarkTrackerImpl<TPartitionKey> Impl_;
    TNotifyHandler NotifyHandler_;
};

} // namespace NYql::NDq
