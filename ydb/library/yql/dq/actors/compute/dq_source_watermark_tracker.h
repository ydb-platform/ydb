#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/system/types.h>

#include <optional>
#include <unordered_map>

namespace NYql::NDq {

template <typename TPartitionKey>
struct TDqSourceWatermarkTracker {
public:
    TDqSourceWatermarkTracker(
            TDuration granularity,
            TInstant startWatermark,
            bool idlePartitionsEnabled,
            TDuration lateArrivalDelay,
            TInstant systemTime)
        : Granularity(granularity)
        , StartWatermark(ToDiscreteTime(startWatermark))
        , IdlePartitionsEnabled(idlePartitionsEnabled)
        , LateArrivalDelay(lateArrivalDelay)
        , LastTimeNotifiedAt(systemTime) {}

    TMaybe<TInstant> NotifyNewPartitionTime(
        const TPartitionKey& partitionKey,
        TInstant partitionTime,
        TInstant systemTime)
    {
        auto [iter, _] = Data.try_emplace(partitionKey);
        if (UpdatePartitionTime(iter->second, partitionTime, systemTime)) {
            return RecalcWatermark();
        }

        return Nothing();
    }

    TMaybe<TInstant> HandleIdleness(TInstant systemTime) {
        if (!Watermark) {
            return Watermark = StartWatermark;
        }

        if (!IdlePartitionsEnabled || !ShouldCheckIdlenessNow(systemTime)) {
            return Nothing();
        }

        if (AllPartitionsAreIdle(systemTime)) {
            return TryProduceFakeWatermark(systemTime);
        }

        return RecalcWatermark();
    }

    TMaybe<TInstant> GetNextIdlenessCheckAt(TInstant systemTime) {
        return IdlePartitionsEnabled
            ? ToDiscreteTime(systemTime + Granularity)
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
        return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity.MicroSeconds());
    }

    bool AllPartitionsAreIdle(TInstant systemTime) const {
        return LastTimeNotifiedAt + LateArrivalDelay <= systemTime;
    }

    bool ShouldCheckIdlenessNow(TInstant systemTime) {
        const auto discreteSystemTime = ToDiscreteTime(systemTime);
        if (discreteSystemTime < NextIdlenessCheckAt) {
            return false;
        }

        NextIdlenessCheckAt = discreteSystemTime + Granularity;
        return true;
    }

    TMaybe<TInstant> RecalcWatermark() {
        const auto maxPartitionSeenTimeIter = MaxElementBy(
            Data.begin(),
            Data.end(),
            [](const auto iter){ return iter.second.Time; });

        if (maxPartitionSeenTimeIter == Data.end()) {
            return Nothing();
        }

        const auto newWatermark = ToDiscreteTime(maxPartitionSeenTimeIter->second.Time - LateArrivalDelay);

        if (!Watermark) {
            // We have to inject start watermark before first data item, because some graph nodes can't start
            // data processing without knowing what the current watermark is.
            return Watermark = Max(StartWatermark, newWatermark);
        }

        if (newWatermark > *Watermark) {
            return Watermark = newWatermark;
        }

        return Nothing();
    }

    bool UpdatePartitionTime(TPartitionState& state, TInstant partitionTime, TInstant sysTime) {
        state.Time = partitionTime;
        state.TimeNotifiedAt = sysTime;
        LastTimeNotifiedAt = sysTime;

        const auto watermark = ToDiscreteTime(partitionTime);
        if (watermark >= state.Watermark) {
            state.Watermark = watermark;
            return true;
        }

        return false;
    }

    TMaybe<TInstant> TryProduceFakeWatermark(TInstant systemTime) {
        const auto fakeWatermark = ToDiscreteTime(systemTime - LateArrivalDelay);
        if (fakeWatermark > Watermark) {
            return Watermark = fakeWatermark;
        }

        return Nothing();
    }

private:
    const TDuration Granularity;
    const TInstant StartWatermark;
    const bool IdlePartitionsEnabled;
    const TDuration LateArrivalDelay;

    THashMap<TPartitionKey, TPartitionState> Data;
    TMaybe<TInstant> Watermark;
    TInstant LastTimeNotifiedAt; // last system time when tracker received notification for any partition
    TMaybe<TInstant> NextIdlenessCheckAt;
};

}
