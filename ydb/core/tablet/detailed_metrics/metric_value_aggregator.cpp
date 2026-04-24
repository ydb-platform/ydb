#include "metric_value_aggregator.h"

namespace NKikimr {

void THistogramMetricAggregator::AggregateValue(NMonitoring::THistogramPtr sourceCounter) {
    Y_ABORT_UNLESS(TargetCounter, "The aggregation process has not started yet");

    // NOTE: There is no good way to combine two histograms, if they use different
    //       buckets. If the two histograms happen to use different buckets,
    //       the metric values will be distorted. This should never happen,
    //       if the detailed metrics are declared correctly.
    auto snapshot1 = TargetCounter->Snapshot();
    auto snapshot2 = sourceCounter->Snapshot();

    Y_ABORT_UNLESS(
        snapshot1->Count() == snapshot2->Count(),
        "Unable to aggregate histograms with a different number of buckets "
        "(%" PRIu64 " vs %" PRIu64 ")",
        snapshot1->Count(),
        snapshot2->Count()
    );

    for (ui32 i = 0; i < snapshot1->Count(); ++i) {
        // NOTE: Notice that the added value is the upper bound for the current
        //       bucket from the TARGET histogram. The added count is the value
        //       for the current bucket from the SOURCE histogram. This allows
        //       for the two histograms to use different units for the bucket boundaries.
        //       This works only if the two histograms have the same NUMBER of buckets.
        //       This is guaranteed by the assert above.
        TargetCounter->Collect(snapshot1->UpperBound(i), snapshot2->Value(i));
    }
}

} // namespace NKikimr
