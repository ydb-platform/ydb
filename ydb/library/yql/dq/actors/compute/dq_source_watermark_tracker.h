#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>
#include <util/string/builder.h>
#include <util/generic/set.h>
#include <ydb/library/yql/dq/actors/compute/dq_common_watermark_tracker.h>

namespace NYql::NDq {

template <typename TPartitionKey>
struct TDqSourceWatermarkTracker {
public:
    TDqSourceWatermarkTracker(
        TDuration granularity,
        bool idlePartitionsEnabled,
        TDuration lateArrivalDelay,
        TDuration idleDelay,
        const TString& logPrefix)
        : Granularity_(granularity)
        , IdlePartitionsEnabled_(idlePartitionsEnabled)
        , LateArrivalDelay_(lateArrivalDelay)
        , IdleDelay_(idleDelay)
        , Tracker_(logPrefix)
    {}

    [[nodiscard]] TMaybe<TInstant> NotifyNewPartitionTime(
        const TPartitionKey& partitionKey,
        TInstant partitionTime,
        TInstant systemTime
    ) {
        const auto watermark = ToDiscreteTime(partitionTime - LateArrivalDelay_);
        return Tracker_.NotifyNewWatermark(partitionKey, watermark, ToNextDiscreteTime(systemTime)).first;
    }

    bool RegisterPartition(const TPartitionKey& partitionKey, TInstant systemTime) {
        return Tracker_.RegisterInput(partitionKey, ToNextDiscreteTime(systemTime), IdlePartitionsEnabled_ ? IdleDelay_ : TDuration::Max());
    }

    [[nodiscard]] TMaybe<TInstant> HandleIdleness(TInstant systemTime) {
        return Tracker_.HandleIdleness(ToDiscreteTime(systemTime));
    }

    [[nodiscard]] TMaybe<TInstant> GetNextIdlenessCheckAt(TInstant systemTime) {
        auto nextCheck = Tracker_.GetNextIdlenessCheckAt();
        return nextCheck ? TMaybe<TInstant>(ToNextDiscreteTime(Max(*nextCheck, systemTime))) : Nothing();
    }

    TMaybe<TDuration> GetWatermarkDiscrepancy() const {
        return Tracker_.GetWatermarkDiscrepancy();
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
    const TDuration IdleDelay_;

    TDqWatermarkTracker<TPartitionKey> Tracker_;
};

}
