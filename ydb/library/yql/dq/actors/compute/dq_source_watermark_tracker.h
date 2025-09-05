#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>

namespace NYql::NDq {

template <typename TPartitionKey>
struct TDqSourceWatermarkTracker {
public:
    TDqSourceWatermarkTracker(
        TDuration granularity,
        bool idlePartitionsEnabled,
        TDuration lateArrivalDelay,
        TInstant systemTime)
        : Granularity_(granularity)
        , IdlePartitionsEnabled_(idlePartitionsEnabled)
        , LateArrivalDelay_(lateArrivalDelay)
        , LastTimeNotifiedAt_(systemTime)
    {}

    [[nodiscard]] TMaybe<TInstant> NotifyNewPartitionTime(
        const TPartitionKey& partitionKey,
        TInstant partitionTime,
        TInstant systemTime
    ) {
        auto [iter, _] = Data_.try_emplace(partitionKey);
        if (UpdatePartitionTime(iter->second, partitionTime, systemTime)) {
            return RecalcWatermark();
        }

        return Nothing();
    }

    [[nodiscard]] TMaybe<TInstant> HandleIdleness(TInstant systemTime) {
        if (!Watermark_) {
            return Nothing();
        }

        if (!IdlePartitionsEnabled_ || !ShouldCheckIdlenessNow(systemTime)) {
            return Nothing();
        }

        if (AllPartitionsAreIdle(systemTime)) {
            return TryProduceFakeWatermark(systemTime);
        }

        return RecalcWatermark();
    }

    [[nodiscard]] TMaybe<TInstant> GetNextIdlenessCheckAt(TInstant systemTime) {
        return IdlePartitionsEnabled_
            ? ToDiscreteTime(systemTime + Granularity_)
            : TMaybe<TInstant>();
    }

private:
    struct TPartitionState {
        TInstant Time;  // partition time, notified outside
        TInstant TimeNotifiedAt; // system time when notification was received
        TInstant Watermark;
    };

private:
    TInstant ToDiscreteTime(TInstant time) const {
        return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity_.MicroSeconds());
    }

    bool AllPartitionsAreIdle(TInstant systemTime) const {
        return LastTimeNotifiedAt_ + LateArrivalDelay_ <= systemTime;
    }

    bool ShouldCheckIdlenessNow(TInstant systemTime) {
        const auto discreteSystemTime = ToDiscreteTime(systemTime);
        if (discreteSystemTime < NextIdlenessCheckAt_) {
            return false;
        }

        NextIdlenessCheckAt_ = discreteSystemTime + Granularity_;
        return true;
    }

    TMaybe<TInstant> RecalcWatermark() {
        const auto maxPartitionSeenTimeIter = MaxElementBy(
            Data_.begin(),
            Data_.end(),
            [](const auto iter){ return iter.second.Time; });

        if (maxPartitionSeenTimeIter == Data_.end()) {
            return Nothing();
        }

        const auto newWatermark = ToDiscreteTime(maxPartitionSeenTimeIter->second.Time - LateArrivalDelay_);

        if (!Watermark_) {
            return Watermark_ = newWatermark;
        }

        if (newWatermark > *Watermark_) {
            return Watermark_ = newWatermark;
        }

        return Nothing();
    }

    bool UpdatePartitionTime(TPartitionState& state, TInstant partitionTime, TInstant systemTime) {
        state.Time = partitionTime;
        state.TimeNotifiedAt = systemTime;
        LastTimeNotifiedAt_ = systemTime;

        const auto watermark = ToDiscreteTime(partitionTime);
        if (watermark >= state.Watermark) {
            state.Watermark = watermark;
            return true;
        }

        return false;
    }

    TMaybe<TInstant> TryProduceFakeWatermark(TInstant systemTime) {
        const auto fakeWatermark = ToDiscreteTime(systemTime - LateArrivalDelay_);
        if (fakeWatermark > Watermark_) {
            return Watermark_ = fakeWatermark;
        }

        return Nothing();
    }

private:
    const TDuration Granularity_;
    const bool IdlePartitionsEnabled_;
    const TDuration LateArrivalDelay_;

    THashMap<TPartitionKey, TPartitionState> Data_;
    TMaybe<TInstant> Watermark_;
    TInstant LastTimeNotifiedAt_; // last system time when tracker received notification for any partition
    TMaybe<TInstant> NextIdlenessCheckAt_;
};

}
