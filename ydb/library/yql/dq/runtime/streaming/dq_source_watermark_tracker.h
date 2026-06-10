#pragma once

#include "dq_watermark_tracker_impl.h"

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>
#include <util/string/builder.h>
#include <util/generic/set.h>

namespace NYql::NDq {

template <typename TPartitionKey>
class TDqSourceWatermarkTracker {
public:
    TDqSourceWatermarkTracker(
        TDuration granularity,
        bool idlePartitionsEnabled,
        TDuration lateArrivalDelay,
        TDuration idleTimeout,
        const TString& logPrefix,
        const ::NMonitoring::TDynamicCounterPtr& counters = {})
        : Granularity_(granularity)
        , IdlePartitionsEnabled_(idlePartitionsEnabled)
        , LateArrivalDelay_(lateArrivalDelay)
        , IdleTimeout_(idleTimeout)
        , Impl_(logPrefix, counters)
    {}

    TDqSourceWatermarkTracker(
        const TString& logPrefix,
        const ::NMonitoring::TDynamicCounterPtr& counters = {})
        : Impl_(logPrefix, counters)
    {}

    void SetSettings(
        TDuration granularity,
        bool idlePartitionsEnabled,
        TDuration lateArrivalDelay,
        TDuration idleTimeout
    ) {
        Granularity_ = granularity;
        IdlePartitionsEnabled_ = idlePartitionsEnabled;
        LateArrivalDelay_ = lateArrivalDelay;
        IdleTimeout_ = idleTimeout;
    }

    using TNotifyHandler = std::function<void(TInstant)>;

    void SetNotifyHandler(TNotifyHandler notifyHandler) {
        NotifyHandler_ = std::move(notifyHandler);
    }

    void SetLogPrefix(const TString& logPrefix) {
        Impl_.SetLogPrefix(logPrefix);
    }

    [[nodiscard]] TMaybe<TInstant> NotifyNewPartitionTime(
        const TPartitionKey& partitionKey,
        TInstant partitionTime,
        TInstant systemTime
    ) {
        const auto watermark = ToDiscreteTime(partitionTime - LateArrivalDelay_);
        return Impl_.NotifyNewWatermark(partitionKey, watermark, systemTime).first;
    }

    bool RegisterPartition(const TPartitionKey& partitionKey, TInstant systemTime) {
        return Impl_.RegisterInput(partitionKey, systemTime, IdlePartitionsEnabled_ ? IdleTimeout_ : TDuration::Max());
    }

    [[nodiscard]] TMaybe<TInstant> HandleIdleness(TInstant systemTime) {
        return Impl_.HandleIdleness(systemTime);
    }

    // returns time for idleness check that should be scheduled now
    [[nodiscard]] TMaybe<TInstant> PrepareIdlenessCheck(TInstant systemTime) {
        if (auto nextCheck = Impl_.GetNextIdlenessCheckAt()) {
            auto notifyTime = Max(*nextCheck, systemTime);
            if (Impl_.AddScheduledIdlenessCheck(notifyTime)) {
                if (NotifyHandler_) {
                    NotifyHandler_(notifyTime);
                }
                return notifyTime;
            }
        }
        return Nothing();
    }

    bool ProcessIdlenessCheck(TInstant notifyTime) {
        return Impl_.RemoveExpiredIdlenessChecks(notifyTime);
    }

    TMaybe<TDuration> GetWatermarkDiscrepancy() const {
        return Impl_.GetWatermarkDiscrepancy();
    }

    void Out(IOutputStream& str) const {
        Impl_.Out(str);
    }

private:
    TInstant ToDiscreteTime(TInstant time) const {
        return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity_.MicroSeconds());
    }

private:
    TDuration Granularity_;
    bool IdlePartitionsEnabled_;
    TDuration LateArrivalDelay_;
    TDuration IdleTimeout_;

    TDqWatermarkTrackerImpl<TPartitionKey> Impl_;
    TNotifyHandler NotifyHandler_;
};

} // namespace NYql::NDq
