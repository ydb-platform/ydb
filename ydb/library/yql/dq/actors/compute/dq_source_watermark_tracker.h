#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>
#include <util/string/builder.h>
#include <util/generic/set.h>
#include <ydb/library/yql/dq/actors/compute/dq_watermark_tracker_impl.h>

namespace NYql::NDq {

template <typename TPartitionKey>
struct TDqSourceWatermarkTracker {
public:
    TDqSourceWatermarkTracker(
        TDuration granularity,
        bool idlePartitionsEnabled,
        TDuration lateArrivalDelay,
        TDuration idleTimeout,
        const TString& logPrefix)
        : Granularity_(granularity)
        , IdlePartitionsEnabled_(idlePartitionsEnabled)
        , LateArrivalDelay_(lateArrivalDelay)
        , IdleTimeout_(idleTimeout)
        , Impl_(logPrefix)
    {}

    [[nodiscard]] TMaybe<TInstant> NotifyNewPartitionTime(
        const TPartitionKey& partitionKey,
        TInstant partitionTime,
        TInstant systemTime
    ) {
        const auto watermark = ToDiscreteTime(partitionTime - LateArrivalDelay_);
        return Impl_.NotifyNewWatermark(partitionKey, watermark, ToNextDiscreteTime(systemTime)).first;
    }

    bool RegisterPartition(const TPartitionKey& partitionKey, TInstant systemTime) {
        return Impl_.RegisterInput(partitionKey, ToNextDiscreteTime(systemTime), IdlePartitionsEnabled_ ? IdleTimeout_ : TDuration::Max());
    }

    [[nodiscard]] TMaybe<TInstant> HandleIdleness(TInstant systemTime) {
        return Impl_.HandleIdleness(ToDiscreteTime(systemTime));
    }

    // returns time for idleness check that should be scheduled now
    [[nodiscard]] TMaybe<TInstant> PrepareIdlenessCheck(TInstant systemTime) {
        if (auto nextCheck = Impl_.GetNextIdlenessCheckAt()) {
            auto notifyTime = ToNextDiscreteTime(Max(*nextCheck, systemTime));
            if (Impl_.AddScheduledIdlenessCheck(notifyTime)) {
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

private:
    TInstant ToDiscreteTime(TInstant time) const {
        return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity_.MicroSeconds());
    }

    TInstant ToNextDiscreteTime(TInstant time) const {
        return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity_.MicroSeconds()) + Granularity_;
    }

private:
    const TDuration Granularity_;
    const bool IdlePartitionsEnabled_;
    const TDuration LateArrivalDelay_;
    const TDuration IdleTimeout_;

    TDqWatermarkTrackerImpl<TPartitionKey> Impl_;
};

}
