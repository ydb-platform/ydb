#include "detailed_values_accumulator.h"

#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>

#include <algorithm>

namespace NKikimr {

namespace {

ui64 GetSimpleValue(const TTabletCountersBase& counters, ui32 slot) {
    return slot < counters.Simple().Size() ? counters.Simple()[slot].Get() : 0;
}

ui64 GetCumulativeValue(const TTabletCountersBase& counters, ui32 slot) {
    return slot < counters.Cumulative().Size() ? counters.Cumulative()[slot].Get() : 0;
}

// Picks the bucket like the explicit monlib histogram HIST(x) of NPrivate::TAggregatedTabletCounters
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

    // One allocation for the pending deltas and the first source (the only one of a PARTITION leaf)
    Data.reserve(GetStateBegin() + Binding->PerSourceStateSize);
    Data.resize(GetStateBegin(), 0);
}

void TDetailedValuesAccumulator::Apply(
    const TTabletKey& tablet,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters,
    TInstant now)
{
    const size_t source = FindSource(tablet);
    if (source == Sources.size() || Sources[source].Key != tablet) {
        Sources.insert(Sources.begin() + source, TSourceHeader{.Key = tablet});
        const size_t stateSize = Binding->PerSourceStateSize;
        Data.insert(Data.begin() + GetStateBegin() + source * stateSize, stateSize, 0);
    }

    // Like NPrivate::TAggregatedTabletCounters::Apply(): a zero duration if the time goes back (saturating)
    auto& header = Sources[source];
    TDuration diff;
    if (header.HasUpdate) {
        diff = now - header.LastUpdate;
    }
    header.LastUpdate = now;
    header.HasUpdate = true;

    const auto& descriptor = *Binding->Descriptor;
    ui64* state = Data.data() + GetStateBegin() + source * Binding->PerSourceStateSize;

    for (const auto& term : Binding->Terms) {
        if (IsSkipped(term)) {
            continue;
        }

        const auto& counters = term.Category == ESourceCounterCategory::SCC_TABLET ? appCounters : executorCounters;

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

        case ESourceOp::HistOfCumulative:
            state[term.StateOffset] = FindPublicBucket(
                term,
                descriptor.Histograms[term.Metric].BucketCount(),
                NPrivate::DifferentiateToPerSecondRate(GetCumulativeValue(counters, term.Slot), diff));
            break;

        case ESourceOp::PercentileNonDerivative:
        case ESourceOp::PercentileDerivative:
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
    // An uninitialized counter changes nothing, like NPrivate::TAggregatedHistogramCounters::SetValue()
    const ui32 rangeCount = percentile.GetRangeCount();
    if (rangeCount == 0) {
        return;
    }

    const size_t bucketCount = Binding->Descriptor->Histograms[term.Metric].BucketCount();
    ui64* buckets = nullptr;

    if (term.Op == ESourceOp::PercentileNonDerivative) {
        buckets = state + term.StateOffset;
        std::fill(buckets, buckets + bucketCount, 0);
    } else {
        buckets = Data.data() + Binding->Descriptor->Rates.size() + term.PendingOffset;
    }

    for (ui32 i = 0; i < rangeCount; ++i) {
        buckets[Min<size_t>(i, bucketCount - 1)] += percentile.GetRangeValue(i);
    }
}

void TDetailedValuesAccumulator::Forget(const TTabletKey& tablet) {
    const size_t source = FindSource(tablet);
    if (source == Sources.size() || Sources[source].Key != tablet) {
        return;
    }

    const size_t stateSize = Binding->PerSourceStateSize;
    const auto state = Data.begin() + GetStateBegin() + source * stateSize;

    Sources.erase(Sources.begin() + source);
    Data.erase(state, state + stateSize);
}

void TDetailedValuesAccumulator::Pack(NKikimrSysView::TDbCounters& out) {
    const auto& descriptor = *Binding->Descriptor;
    const size_t rateCount = descriptor.Rates.size();
    const size_t stateSize = Binding->PerSourceStateSize;
    const size_t sourceCount = Sources.size();
    const ui64* states = Data.data() + GetStateBegin();

    out.Clear();

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

    out.SetCumulativeCount(rateCount);

    for (size_t metric = 0; metric < rateCount; ++metric) {
        if (Data[metric]) {
            out.AddCumulative(metric);
            out.AddCumulative(Data[metric]);
            Data[metric] = 0;
        }
    }

    TVector<ui64> buckets;

    for (ui32 metric = 0; metric < descriptor.Histograms.size(); ++metric) {
        const size_t bucketCount = descriptor.Histograms[metric].BucketCount();
        const bool nonDerivative = descriptor.Histograms[metric].NonDerivative;

        auto* histogram = out.AddHistogram();
        histogram->SetBucketsCount(bucketCount);
        if (nonDerivative) {
            histogram->SetNonDerivative(true);
        }

        buckets.assign(bucketCount, 0);

        ui64* pending = nullptr;

        for (const auto& term : Binding->Terms) {
            if (term.Kind != EMetricKind::Histogram || term.Metric != metric || IsSkipped(term)) {
                continue;
            }

            switch (term.Op) {
            case ESourceOp::HistOfSimple:
            case ESourceOp::HistOfCumulative:
                for (size_t source = 0; source < sourceCount; ++source) {
                    const ui64 bucket = states[source * stateSize + term.StateOffset];
                    ++buckets[Min<ui64>(bucket, bucketCount - 1)];
                }
                break;

            case ESourceOp::PercentileNonDerivative:
                for (size_t source = 0; source < sourceCount; ++source) {
                    const ui64* sourceBuckets = states + source * stateSize + term.StateOffset;
                    for (size_t bucket = 0; bucket < bucketCount; ++bucket) {
                        buckets[bucket] += sourceBuckets[bucket];
                    }
                }
                break;

            case ESourceOp::PercentileDerivative:
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
    return bytes;
}

size_t TDetailedValuesAccumulator::FindSource(const TTabletKey& tablet) const {
    return LowerBoundBy(Sources.begin(), Sources.end(), tablet, [](const TSourceHeader& source) -> const TTabletKey& {
        return source.Key;
    }) - Sources.begin();
}

size_t TDetailedValuesAccumulator::GetStateBegin() const {
    return Binding->Descriptor->Rates.size() + Binding->PendingHistSize;
}

} // namespace NKikimr
