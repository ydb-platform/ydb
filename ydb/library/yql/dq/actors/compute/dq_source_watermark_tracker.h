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
            ui32 expectedPartitionsCount)
        : Granularity(granularity)
        , StartWatermark(ToDiscreteTime(startWatermark))
        , ExpectedPartitionsCount(expectedPartitionsCount) {}

    TMaybe<TInstant> NotifyNewPartitionTime(const TPartitionKey& partitionKey, TInstant time) {
        auto granularPartitionTime = ToDiscreteTime(time);

        auto iter = Data.find(partitionKey);
        if (iter == Data.end()) {
            Data[partitionKey] = granularPartitionTime;
            return RecalcWatermark();
        }

        if (granularPartitionTime <= iter->second) {
            return Nothing();
        }

        iter->second = granularPartitionTime;
        return RecalcWatermark();
    }

private:
    TInstant ToDiscreteTime(TInstant time) const {
        return TInstant::MicroSeconds(time.MicroSeconds() - time.MicroSeconds() % Granularity.MicroSeconds());
    }

    TMaybe<TInstant> RecalcWatermark() {
        if (!Watermark) {
            // We have to inject start watermark before first data item, because some graph nodes can't start
            // data processing without knowing what the current watermark is.
            Watermark = StartWatermark;
            return Watermark;
        }

        if (Data.size() < ExpectedPartitionsCount) {
            // Each partition should notify time at least once before we are able to move watermark
            return Nothing();
        }

        auto minTime = Data.begin()->second;
        for (const auto& [_, time] : Data) {
            if (time < minTime) {
                minTime = time;
            }
        }

        if (minTime > Watermark) {
            Watermark = minTime;
            return Watermark;
        }

        return Nothing();
    }

private:
    const TDuration Granularity;
    const TInstant StartWatermark;
    const ui32 ExpectedPartitionsCount;

    THashMap<TPartitionKey, TInstant> Data;
    TMaybe<TInstant> Watermark;
};

}
