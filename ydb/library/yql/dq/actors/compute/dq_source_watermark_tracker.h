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
        auto [iter, inserted] = Data_.try_emplace(partitionKey);
        bool pendingPartitionsBecameEmpty = false;
        if (inserted) {
            auto removed = PendingPartitions_.erase(partitionKey);
            Y_DEBUG_ABORT_UNLESS(removed);
            pendingPartitionsBecameEmpty = PendingPartitions_.empty();
        }
        if ((UpdatePartitionTime(iter->second, partitionTime, systemTime) && PendingPartitions_.empty()) || pendingPartitionsBecameEmpty) {
            return RecalcWatermark(systemTime);
        }

        return Nothing();
    }

    bool RegisterPartition(const TPartitionKey& partitionKey) {
        if (Data_.find(partitionKey) != Data_.end()) {
            return false;
        }
        auto [_, inserted] = PendingPartitions_.insert(partitionKey);
        return inserted;
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

        return RecalcWatermark(systemTime);
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

    TMaybe<TInstant> RecalcWatermark(TInstant systemTime) {
        const auto minPartitionSeenTimeIter = MinElementBy(
            Data_.begin(),
            Data_.end(),
            [](const auto iter){ return iter.second.Watermark; });

        if (minPartitionSeenTimeIter == Data_.end()) {
            return Nothing();
        }

        const auto newWatermark = minPartitionSeenTimeIter->second.Watermark;

        if (!Watermark_) {
            Watermark_ = newWatermark;
        } else if (newWatermark > *Watermark_) {
            Watermark_ = newWatermark;
        } else {
            return Nothing();
        }
        LastTimeNotifiedAt_ = systemTime;
        return Watermark_;
    }

    bool UpdatePartitionTime(TPartitionState& state, TInstant partitionTime, TInstant systemTime) {
        state.Time = partitionTime;
        state.TimeNotifiedAt = systemTime;

        const auto watermark = ToDiscreteTime(partitionTime - LateArrivalDelay_);
        if (state.Watermark < watermark) {
            auto oldWatermark = std::exchange(state.Watermark, watermark);
            return !Watermark_ || *Watermark_ == oldWatermark;
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
    THashSet<TPartitionKey> PendingPartitions_;
    TMaybe<TInstant> Watermark_;
    TInstant LastTimeNotifiedAt_; // last system time when tracker received notification for any partition
    TMaybe<TInstant> NextIdlenessCheckAt_;
};

}
