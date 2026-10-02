#include "detailed_values_accumulator.h"

#include <ydb/core/protos/sys_view.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>

#include <algorithm>

namespace NKikimr {

namespace {

/**
 * @return The value of the simple counter at the given slot, or zero if there is no such slot
 */
ui64 GetSimpleValue(const TTabletCountersBase& counters, ui32 slot) {
    return slot < counters.Simple().Size() ? counters.Simple()[slot].Get() : 0;
}

/**
 * @return The value of the cumulative counter at the given slot (the delta since the previous
 *         report of the tablet), or zero if there is no such slot
 */
ui64 GetCumulativeValue(const TTabletCountersBase& counters, ui32 slot) {
    return slot < counters.Cumulative().Size() ? counters.Cumulative()[slot].Get() : 0;
}

/**
 * @return The public bucket of one observation of the HIST(x) term: the first source bucket,
 *         whose upper bound is not less than the observation (the same way as the explicit
 *         monlib histogram HIST(x) of NPrivate::TAggregatedTabletCounters collects it),
 *         a source bucket beyond the last public one is the last public one
 */
ui64 FindPublicBucket(const TBoundTerm& term, size_t bucketCount, ui64 value) {
    const auto& bounds = term.SourceBounds;
    const size_t bucket = LowerBound(bounds.begin(), bounds.end(), static_cast<double>(value)) - bounds.begin();
    return Min(bucket, bucketCount - 1);
}

} // namespace

TDetailedValuesAccumulator::TDetailedValuesAccumulator(const TDetailedMetricsBinding* binding, bool skipLeaderOnly)
    : Binding(binding)
    , SkipLeaderOnly(skipLeaderOnly)
{
    Y_ABORT_UNLESS(Binding && Binding->Descriptor, "the binding must not be null");

    // The pending deltas and the state of the first source (the only one of a PARTITION leaf)
    // fit into a single allocation
    Data.reserve(GetStateBegin() + Binding->PerSourceStateSize);
    Data.resize(GetStateBegin(), 0);
}

void TDetailedValuesAccumulator::Apply(
    const TTabletKey& tablet,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters,
    TInstant now)
{
    ui32 source = FindSource(tablet);
    if (source == NoSource) {
        source = AddSource(tablet);
    }

    // The same as NPrivate::TAggregatedTabletCounters::Apply(): no duration on the first report
    // of the source, a zero duration (the subtraction saturates) if the time goes back,
    // and the time of the report is kept in any case
    auto& header = Sources[source];
    TDuration diff;
    if (header.HasUpdate) {
        diff = now - header.LastUpdate;
    }
    header.LastUpdate = now;
    header.HasUpdate = true;

    const auto& descriptor = *Binding->Descriptor;
    ui64* state = Data.data() + GetStateBegin() + static_cast<size_t>(source) * Binding->PerSourceStateSize;

    for (const auto& term : Binding->Terms) {
        if (IsSkipped(term)) {
            continue;
        }

        const auto& counters = term.Bank == EBank::Executor ? executorCounters : appCounters;

        switch (term.Op) {
        case ESourceOp::SimpleSum:
        case ESourceOp::SimpleMax:
            state[term.StateOffset] = GetSimpleValue(counters, term.Slot);
            break;

        case ESourceOp::CumulativeDelta:
            Data[term.Metric] += GetCumulativeValue(counters, term.Slot);
            break;

        case ESourceOp::HistOfSimple:
            state[term.StateOffset] = FindPublicBucket(
                term,
                descriptor.Histograms[term.Metric].BucketCount(),
                GetSimpleValue(counters, term.Slot));
            break;

        case ESourceOp::HistOfCumulative: {
            const ui64 valueDiff = GetCumulativeValue(counters, term.Slot);

            // NOTE: The very same expression as NPrivate::TAggregatedTabletCounters::Apply(),
            //       including its overflow for huge deltas
            ui64 rate = 0;
            if (diff) {
                rate = valueDiff * 1000000 / diff.MicroSeconds(); // differentiate value to per second rate
            }

            state[term.StateOffset] = FindPublicBucket(
                term,
                descriptor.Histograms[term.Metric].BucketCount(),
                rate);
            break;
        }

        case ESourceOp::PercentileLevel:
        case ESourceOp::PercentileIncrement:
            if (term.Slot < counters.Percentile().Size()) {
                ApplyPercentile(term, counters.Percentile()[term.Slot], state);
            }
            break;
        }
    }
}

void TDetailedValuesAccumulator::ApplyPercentile(
    const TBoundTerm& term,
    const TTabletPercentileCounter& percentile,
    ui64* state)
{
    // An uninitialized percentile counter has nothing to read: the level of the source stays
    // as it was, the same way as NPrivate::TAggregatedHistogramCounters::SetValue() ignores it
    const ui32 rangeCount = percentile.GetRangeCount();
    if (rangeCount == 0) {
        return;
    }

    // The source buckets beyond the last public one are counted in the last public bucket
    const size_t bucketCount = Binding->Descriptor->Histograms[term.Metric].BucketCount();
    ui64* buckets = nullptr;

    if (term.Op == ESourceOp::PercentileLevel) {
        // The latest buckets of the source replace its previous ones
        buckets = state + term.StateOffset;
        std::fill(buckets, buckets + bucketCount, 0);
    } else {
        // The reported buckets are the increments since the previous report of the source
        buckets = Data.data() + Binding->Descriptor->Rates.size() + term.PendingOffset;
    }

    for (ui32 i = 0; i < rangeCount; ++i) {
        buckets[Min<size_t>(i, bucketCount - 1)] += percentile.GetRangeValue(i);
    }
}

void TDetailedValuesAccumulator::Forget(const TTabletKey& tablet) {
    const ui32 source = FindSource(tablet);
    if (source == NoSource) {
        return;
    }

    const ui32 last = Sources.size() - 1;
    const size_t stateSize = Binding->PerSourceStateSize;

    if (Index) {
        Index->erase(tablet);
    }

    // Swap-remove: the last source takes the place of the forgotten one
    if (source != last) {
        Sources[source] = Sources[last];

        ui64* states = Data.data() + GetStateBegin();
        std::copy(states + last * stateSize, states + (last + 1) * stateSize, states + source * stateSize);

        if (Index) {
            (*Index)[Sources[source].Key] = source;
        }
    }

    Sources.pop_back();
    Data.resize(Data.size() - stateSize);
}

void TDetailedValuesAccumulator::Pack(NKikimrSysView::TDbCounters& out) {
    const auto& descriptor = *Binding->Descriptor;
    const size_t rateCount = descriptor.Rates.size();
    const size_t stateSize = Binding->PerSourceStateSize;
    const size_t sourceCount = Sources.size();
    const ui64* states = Data.data() + GetStateBegin();

    out.Clear();

    // Gauges: the sum (the maximum) of the latest values of the live sources, summed over the terms
    auto* simple = out.MutableSimple();
    simple->Resize(static_cast<int>(descriptor.Gauges.size()), 0);

    for (const auto& term : Binding->Terms) {
        if (term.Kind != EMetricKind::Gauge || IsSkipped(term)) {
            continue;
        }

        ui64 value = 0;
        for (size_t source = 0; source < sourceCount; ++source) {
            const ui64 sourceValue = states[source * stateSize + term.StateOffset];
            value = term.Op == ESourceOp::SimpleMax ? Max(value, sourceValue) : value + sourceValue;
        }

        (*simple)[term.Metric] += value;
    }

    // Rates: the pending deltas, drained
    out.SetCumulativeCount(rateCount);

    for (size_t metric = 0; metric < rateCount; ++metric) {
        if (Data[metric]) {
            out.AddCumulative(metric);
            out.AddCumulative(Data[metric]);
            Data[metric] = 0;
        }
    }

    // Histograms: every histogram metric is present, even if it is empty
    TVector<ui64> buckets;

    for (ui32 metric = 0; metric < descriptor.Histograms.size(); ++metric) {
        const size_t bucketCount = descriptor.Histograms[metric].BucketCount();
        const bool isLevel = Binding->HistIsLevel[metric];

        auto* histogram = out.AddHistogram();
        histogram->SetBucketsCount(bucketCount);
        if (isLevel) {
            histogram->SetNonDerivative(true);
        }

        buckets.assign(bucketCount, 0);

        // The pending increment buckets are shared by all terms of the metric
        ui64* pending = nullptr;

        for (const auto& term : Binding->Terms) {
            if (term.Kind != EMetricKind::Histogram || term.Metric != metric || IsSkipped(term)) {
                continue;
            }

            switch (term.Op) {
            case ESourceOp::HistOfSimple:
            case ESourceOp::HistOfCumulative:
                // One observation per live source
                for (size_t source = 0; source < sourceCount; ++source) {
                    const ui64 bucket = states[source * stateSize + term.StateOffset];
                    ++buckets[Min<ui64>(bucket, bucketCount - 1)];
                }
                break;

            case ESourceOp::PercentileLevel:
                for (size_t source = 0; source < sourceCount; ++source) {
                    const ui64* sourceBuckets = states + source * stateSize + term.StateOffset;
                    for (size_t bucket = 0; bucket < bucketCount; ++bucket) {
                        buckets[bucket] += sourceBuckets[bucket];
                    }
                }
                break;

            case ESourceOp::PercentileIncrement:
                pending = Data.data() + rateCount + term.PendingOffset;
                break;

            case ESourceOp::SimpleSum:
            case ESourceOp::SimpleMax:
            case ESourceOp::CumulativeDelta:
                break;
            }
        }

        if (pending) {
            for (size_t bucket = 0; bucket < bucketCount; ++bucket) {
                buckets[bucket] += pending[bucket];
                pending[bucket] = 0;
            }
        }

        for (size_t bucket = 0; bucket < bucketCount; ++bucket) {
            if (buckets[bucket]) {
                histogram->AddBuckets(bucket);
                histogram->AddBuckets(buckets[bucket]);
            }
        }
    }
}

size_t TDetailedValuesAccumulator::GetAllocatedBytes() const {
    size_t bytes = sizeof(*this);

    bytes += Sources.capacity() * sizeof(TSourceHeader);
    bytes += Data.capacity() * sizeof(ui64);

    if (Index) {
        // The map itself, its bucket array and one node (the value and the next pointer) per source
        bytes += sizeof(*Index);
        bytes += Index->bucket_count() * sizeof(void*);
        bytes += Index->size() * (sizeof(std::pair<const TTabletKey, ui32>) + sizeof(void*));
    }

    return bytes;
}

ui32 TDetailedValuesAccumulator::FindSource(const TTabletKey& tablet) const {
    if (Index) {
        const auto it = Index->find(tablet);
        return it != Index->end() ? it->second : NoSource;
    }

    for (ui32 source = 0; source < Sources.size(); ++source) {
        if (Sources[source].Key == tablet) {
            return source;
        }
    }

    return NoSource;
}

ui32 TDetailedValuesAccumulator::AddSource(const TTabletKey& tablet) {
    const ui32 source = Sources.size();

    Sources.push_back(TSourceHeader{.Key = tablet});
    Data.resize(Data.size() + Binding->PerSourceStateSize, 0);

    if (Index) {
        Index->emplace(tablet, source);
    } else if (Sources.size() > IndexThreshold) {
        // Too many sources for a linear scan: index all of them from now on
        Index = MakeHolder<THashMap<TTabletKey, ui32>>();
        Index->reserve(Sources.size());

        for (ui32 i = 0; i < Sources.size(); ++i) {
            Index->emplace(Sources[i].Key, i);
        }
    }

    return source;
}

size_t TDetailedValuesAccumulator::GetStateBegin() const {
    return Binding->Descriptor->Rates.size() + Binding->PendingHistSize;
}

} // namespace NKikimr
