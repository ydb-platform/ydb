#include "public_metrics_bucket.h"

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>

#include <utility>

namespace NKikimr {

namespace {

/**
 * @return The value, which an explicit histogram over the public bounds of the metric
 *         counts in the given bucket: the upper bound of the bucket, or Max<double>()
 *         for the implicit +Inf bucket (the same value as the YDB metrics mapper collects
 *         for the bucket, the upper bound of the bucket of the target histogram)
 */
double GetBucketUpperBound(const TMetricSpec& spec, size_t bucket) {
    return bucket < spec.Bounds.size() ? static_cast<double>(spec.Bounds[bucket]) : Max<double>();
}

/**
 * @return The bounds of the explicit histogram target of the metric: the public bounds
 *         (the explicit histogram adds the +Inf bucket itself), or the single placeholder
 *         bound 0, if the public bounds cannot make an explicit histogram (no bounds,
 *         unsorted bounds or too many bounds), so that such a metric (which failed
 *         the validation of the descriptor) still gets its target instead of an exception
 */
NMonitoring::TBucketBounds MakeTargetBounds(const TMetricSpec& spec) {
    // NOTE: The same checks as NMonitoring::ExplicitHistogram() ensures
    const bool usable = !spec.Bounds.empty()
        && spec.Bounds.size() <= NMonitoring::HISTOGRAM_MAX_BUCKETS_COUNT
        && IsSorted(spec.Bounds.begin(), spec.Bounds.end());

    if (!usable) {
        return NMonitoring::TBucketBounds{0.0};
    }

    return NMonitoring::TBucketBounds(spec.Bounds.begin(), spec.Bounds.end());
}

template <class T>
size_t GetCapacityBytes(const TVector<T>& values) {
    return values.capacity() * sizeof(T);
}

} // namespace

TPublicTargets::TPublicTargets(
    const TDetailedMetricsDescriptor& descriptor,
    NMonitoring::TDynamicCounterPtr group,
    EYdbMetricNameScope scope,
    bool skipLeaderOnly)
{
    // NOTE: PartitionName is MakeYdbMetricName(Name, EYdbMetricNameScope::Partition),
    //       and the aggregate name is the name itself, the same names as the YDB metrics
    //       mapper creates for the same scope
    const auto getName = [scope](const TMetricSpec& spec) -> const TString& {
        return scope == EYdbMetricNameScope::Partition ? spec.PartitionName : spec.Name;
    };

    // Leave the target null for LeaderOnly metrics when skipLeaderOnly is true
    const auto isSkipped = [skipLeaderOnly](const TMetricSpec& spec) {
        return skipLeaderOnly && spec.LeaderOnly;
    };

    Gauges.reserve(descriptor.Gauges.size());

    for (const auto& spec : descriptor.Gauges) {
        auto& target = Gauges.emplace_back();

        if (!isSkipped(spec)) {
            target = group->GetNamedCounter("name", getName(spec), false /* derivative */);
        }
    }

    Rates.reserve(descriptor.Rates.size());

    for (const auto& spec : descriptor.Rates) {
        auto& target = Rates.emplace_back();

        if (!isSkipped(spec)) {
            target = group->GetNamedCounter("name", getName(spec), true /* derivative */);
        }
    }

    Histograms.reserve(descriptor.Histograms.size());

    for (const auto& spec : descriptor.Histograms) {
        auto& target = Histograms.emplace_back();

        if (!isSkipped(spec)) {
            target = group->GetNamedHistogram(
                "name",
                getName(spec),
                NMonitoring::ExplicitHistogram(MakeTargetBounds(spec)),
                false /* derivative */
            );
        }
    }
}

TPublicBucket::TPublicBucket(
    const TDetailedMetricsDescriptor& descriptor,
    NMonitoring::TDynamicCounterPtr group,
    bool isPartitionBucket,
    bool skipLeaderOnly)
    : Desc(&descriptor)
    , IsPartitionBucket(isPartitionBucket)
    , RateTotals(descriptor.Rates.size(), 0)
    , IncrementTotals(descriptor.Histograms.size())
    , Targets(
        descriptor,
        group,
        isPartitionBucket ? EYdbMetricNameScope::Partition : EYdbMetricNameScope::Aggregate,
        skipLeaderOnly)
{
    for (size_t i = 0; i < descriptor.Histograms.size(); ++i) {
        const auto& spec = descriptor.Histograms[i];

        if (!spec.IsLevel) {
            IncrementTotals[i].resize(spec.BucketCount(), 0);
        }
    }
}

void TPublicBucket::Apply(ui32 nodeId, const NKikimrSysView::TDbCounters& values) {
    auto& node = GetOrAddNode(nodeId);

    // Gauges: the absolute values of the node, dense; a missing slot is zero,
    // an extra slot is ignored
    const auto& simple = values.GetSimple();

    for (size_t i = 0; i < node.Gauges.size(); ++i) {
        node.Gauges[i] = i < static_cast<size_t>(simple.size()) ? simple[i] : 0;
    }

    // Rates: sparse (slot, delta) pairs; an unknown slot and an odd tail are ignored
    //
    // NOTE: CumulativeCount is not used: the number of the rates is known from
    //       the descriptor, and nothing is sized by the payload
    const auto& cumulative = values.GetCumulative();

    for (int pair = 0; pair + 1 < cumulative.size(); pair += 2) {
        const ui64 index = cumulative[pair];

        if (index < RateTotals.size()) {
            RateTotals[index] += cumulative[pair + 1];
        }
    }

    // Histograms: one entry per metric; an extra entry is ignored,
    // a missing entry is an empty histogram (no level and no increments)
    const size_t histogramCount = Desc->Histograms.size();

    for (size_t i = 0; i < histogramCount; ++i) {
        if (i < static_cast<size_t>(values.HistogramSize())) {
            ApplyHistogram(nodeId, i, values.GetHistogram(i), node);
        } else {
            Fill(node.LevelHists[i].begin(), node.LevelHists[i].end(), 0);
        }
    }
}

void TPublicBucket::ApplyHistogram(
    ui32 nodeId,
    size_t index,
    const NKikimrSysView::TDbCounters::THistogram& histogram,
    TNodeLevels& node)
{
    const auto& spec = Desc->Histograms[index];
    const bool marked = histogram.GetNonDerivative();
    auto& level = node.LevelHists[index];

    if (marked != spec.IsLevel) {
        if (spec.StaticLevel) {
            // An unmarked HIST(x) payload is a delta, which cannot be applied without
            // the baseline it was taken against, so the node contributes nothing
            // to the histogram until its next marked report
            Fill(level.begin(), level.end(), 0);

            if (!WarnedUnmarkedLevel) {
                WarnedUnmarkedLevel = true;
                Warnings.push_back(TStringBuilder()
                    << "node " << nodeId << ": the level histogram '" << spec.Name << "' (#" << index
                    << ") is not marked NonDerivative, the histogram of the node is cleared");
            }
        } else if (!WarnedMarkMismatch) {
            WarnedMarkMismatch = true;
            Warnings.push_back(TStringBuilder()
                << "node " << nodeId << ": the histogram '" << spec.Name << "' (#" << index
                << ") holds " << (spec.IsLevel ? "a level" : "increments")
                << ", but its payload is " << (marked ? "" : "not ")
                << "marked NonDerivative, the payload is ignored");
        }

        return;
    }

    // The payload never has more buckets than the metric has: the extra buckets
    // of the payload (for example, from a node with more bounds) are ignored
    const ui64 bucketCount = Min<ui64>(histogram.GetBucketsCount(), spec.BucketCount());
    const auto& buckets = histogram.GetBuckets();

    if (spec.IsLevel) {
        // The full current value of the node replaces the previous one
        Fill(level.begin(), level.end(), 0);

        for (int pair = 0; pair + 1 < buckets.size(); pair += 2) {
            if (buckets[pair] < bucketCount) {
                level[buckets[pair]] = buckets[pair + 1];
            }
        }
    } else {
        auto& totals = IncrementTotals[index];

        for (int pair = 0; pair + 1 < buckets.size(); pair += 2) {
            if (buckets[pair] < bucketCount) {
                totals[buckets[pair]] += buckets[pair + 1];
            }
        }
    }
}

bool TPublicBucket::DropNode(ui32 nodeId) {
    for (size_t i = 0; i < PerNode.size(); ++i) {
        if (PerNode[i].NodeId == nodeId) {
            if (i + 1 != PerNode.size()) {
                PerNode[i] = std::move(PerNode.back());
            }

            PerNode.pop_back();
            break;
        }
    }

    return PerNode.empty();
}

void TPublicBucket::Publish() {
    for (size_t i = 0; i < Targets.Gauges.size(); ++i) {
        const auto& target = Targets.Gauges[i];

        if (!target) {
            continue;
        }

        // A leaf is reported by more than one node only while the partition moves,
        // so it takes the maximum to avoid doubling the gauge. The partials of a table
        // on different nodes come from different tablets, so they are summed up,
        // unless every source of the gauge is a maximum itself
        const bool combineByMax = IsPartitionBucket || Desc->Gauges[i].CombineByMax;
        ui64 value = 0;

        for (const auto& node : PerNode) {
            value = combineByMax ? Max(value, node.Gauges[i]) : value + node.Gauges[i];
        }

        target->Set(value);
    }

    for (size_t i = 0; i < Targets.Rates.size(); ++i) {
        if (const auto& target = Targets.Rates[i]) {
            target->Set(RateTotals[i]);
        }
    }

    for (size_t i = 0; i < Targets.Histograms.size(); ++i) {
        const auto& target = Targets.Histograms[i];

        if (!target) {
            continue;
        }

        const auto& spec = Desc->Histograms[i];

        // NOTE: The histogram is rebuilt in place, the same way as the YDB metrics
        //       mapper does it (see THistogramMetricAggregator)
        target->Reset();

        for (size_t bucket = 0; bucket < spec.BucketCount(); ++bucket) {
            ui64 count = 0;

            if (spec.IsLevel) {
                for (const auto& node : PerNode) {
                    count += node.LevelHists[i][bucket];
                }
            } else {
                count = IncrementTotals[i][bucket];
            }

            if (count) {
                target->Collect(GetBucketUpperBound(spec, bucket), count);
            }
        }
    }
}

TVector<TString> TPublicBucket::TakeWarnings() {
    return std::exchange(Warnings, {});
}

size_t TPublicBucket::GetAllocatedBytes() const {
    size_t bytes = sizeof(*this);

    bytes += GetCapacityBytes(RateTotals);
    bytes += GetCapacityBytes(IncrementTotals);

    for (const auto& totals : IncrementTotals) {
        bytes += GetCapacityBytes(totals);
    }

    bytes += GetCapacityBytes(PerNode);

    for (const auto& node : PerNode) {
        bytes += GetCapacityBytes(node.Gauges);
        bytes += GetCapacityBytes(node.LevelHists);

        for (const auto& level : node.LevelHists) {
            bytes += GetCapacityBytes(level);
        }
    }

    bytes += GetCapacityBytes(Targets.Gauges);
    bytes += GetCapacityBytes(Targets.Rates);
    bytes += GetCapacityBytes(Targets.Histograms);
    bytes += GetCapacityBytes(Warnings);

    for (const auto& warning : Warnings) {
        bytes += warning.capacity();
    }

    return bytes;
}

TPublicBucket::TNodeLevels& TPublicBucket::GetOrAddNode(ui32 nodeId) {
    for (auto& node : PerNode) {
        if (node.NodeId == nodeId) {
            return node;
        }
    }

    auto& node = PerNode.emplace_back();
    node.NodeId = nodeId;
    node.Gauges.resize(Desc->Gauges.size(), 0);
    node.LevelHists.resize(Desc->Histograms.size());

    for (size_t i = 0; i < Desc->Histograms.size(); ++i) {
        const auto& spec = Desc->Histograms[i];

        if (spec.IsLevel) {
            node.LevelHists[i].resize(spec.BucketCount(), 0);
        }
    }

    return node;
}

} // namespace NKikimr
