#include "detailed_metrics_binding.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <util/string/builder.h>

#include <algorithm>
#include <iterator>

namespace NKikimr {

namespace {

constexpr ui32 NO_SLOT = TBoundTerm::NoSlot;

TSourceRef ParseSourceRef(TStringBuf text, ESourceCounterCategory category) {
    static constexpr std::pair<TStringBuf, ESourceWrapper> wrappers[] = {
        {"SUM(", ESourceWrapper::Sum},
        {"MAX(", ESourceWrapper::Max},
        {"HIST(", ESourceWrapper::Hist},
    };

    for (const auto& [prefix, wrapper] : wrappers) {
        TStringBuf name = text;
        if (name.SkipPrefix(prefix) && name.ChopSuffix(")")) {
            return TSourceRef{category, wrapper, TString(name)};
        }
    }

    return TSourceRef{category, ESourceWrapper::None, TString(text)};
}

/**
 * Build the specifications of the metrics of one kind from their enum, parsed the same way
 * as for the YDB metrics mapper: a malformed definition aborts there.
 */
template <const NProtoBuf::EnumDescriptor* Desc()>
TVector<TMetricSpec> BuildMetricSpecs(EMetricKind kind) {
    const auto* opts = NAux::GetAppOpts<Desc, true /* ParseSourceCounters */>();

    TVector<TMetricSpec> specs;
    specs.reserve(opts->Size);

    for (size_t i = 0; i < opts->Size; ++i) {
        TVector<TSourceRef> sources;
        for (const auto& source : opts->GetSourceCounters(i)) {
            sources.push_back(ParseSourceRef(source.GetName(), source.GetCategory()));
        }

        TVector<ui64> bounds;
        if (kind == EMetricKind::Histogram) {
            for (const auto& range : opts->GetRanges(i)) {
                bounds.push_back(range.RangeVal);
            }
        }

        specs.push_back(MakeMetricSpec(
            kind,
            opts->GetNames()[i],
            opts->GetLeaderOnly(i),
            std::move(sources),
            std::move(bounds),
            kind == EMetricKind::Histogram && opts->GetIntegral(i)));
    }

    return specs;
}

template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc()>
TDetailedMetricsDescriptor BuildDescriptor(TTabletTypes::EType type) {
    return TDetailedMetricsDescriptor{
        .Type = type,
        .Gauges = BuildMetricSpecs<SimpleDesc>(EMetricKind::Gauge),
        .Rates = BuildMetricSpecs<CumulativeDesc>(EMetricKind::Rate),
        .Histograms = BuildMetricSpecs<PercentileDesc>(EMetricKind::Histogram),
    };
}

enum class ECounterArray {
    Simple,
    Cumulative,
    Percentile,
};

ui32 FindCounter(const TTabletCountersBase& counters, ECounterArray array, TStringBuf name) {
    const auto find = [name](ui32 size, auto getName) {
        for (ui32 slot = 0; slot < size; ++slot) {
            const char* counterName = getName(slot);
            if (counterName && name == counterName) {
                return slot;
            }
        }
        return NO_SLOT;
    };

    switch (array) {
    case ECounterArray::Simple:
        return find(counters.Simple().Size(), [&](ui32 slot) { return counters.SimpleCounterName(slot); });
    case ECounterArray::Cumulative:
        return find(counters.Cumulative().Size(), [&](ui32 slot) { return counters.CumulativeCounterName(slot); });
    case ECounterArray::Percentile:
        return find(counters.Percentile().Size(), [&](ui32 slot) { return counters.PercentileCounterName(slot); });
    }

    Y_ABORT("unexpected counter array %d", static_cast<int>(array));
}

class TBinder {
public:
    TBinder(
        const TDetailedMetricsDescriptor& descriptor,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TDetailedMetricsBinding& binding)
        : ExecutorCounters(executorCounters)
        , AppCounters(appCounters)
        , Binding(binding)
    {
        Binding.Descriptor = &descriptor;
        Binding.LayoutSizes = TDetailedMetricsBinding::GetLayoutSizes(executorCounters, appCounters);

        BindMetrics(EMetricKind::Gauge, descriptor.Gauges);
        BindMetrics(EMetricKind::Rate, descriptor.Rates);
        BindMetrics(EMetricKind::Histogram, descriptor.Histograms);
    }

private:
    void BindMetrics(EMetricKind kind, const TVector<TMetricSpec>& specs) {
        for (ui32 metric = 0; metric < specs.size(); ++metric) {
            const auto& spec = specs[metric];

            ui32 pendingOffset = NO_SLOT;

            for (const auto& source : spec.Sources) {
                TBoundTerm term;
                term.Kind = kind;
                term.Metric = metric;
                term.Bank = source.Category == ESourceCounterCategory::SCC_TABLET ? EBank::App : EBank::Executor;
                term.LeaderOnly = spec.LeaderOnly;

                if (!BindTerm(spec, source, term)) {
                    continue;
                }

                AllocateState(spec, term);

                if (term.Op == ESourceOp::PercentileIncrement) {
                    if (pendingOffset == NO_SLOT) {
                        pendingOffset = Binding.PendingHistSize;
                        Binding.PendingHistSize += spec.BucketCount();
                    }
                    term.PendingOffset = pendingOffset;
                }

                Binding.Terms.push_back(std::move(term));
            }
        }
    }

    bool BindTerm(const TMetricSpec& spec, const TSourceRef& source, TBoundTerm& term) {
        const auto& counters = term.Bank == EBank::Executor ? ExecutorCounters : AppCounters;

        switch (term.Kind) {
        case EMetricKind::Gauge:
            if (source.Wrapper == ESourceWrapper::Sum || source.Wrapper == ESourceWrapper::Max) {
                term.Op = source.Wrapper == ESourceWrapper::Max ? ESourceOp::SimpleMax : ESourceOp::SimpleSum;
                return Find(spec, source, counters, ECounterArray::Simple, source.Name, term.Slot);
            }
            break;

        case EMetricKind::Rate:
            if (source.Wrapper == ESourceWrapper::None) {
                term.Op = ESourceOp::CumulativeDelta;
                return Find(spec, source, counters, ECounterArray::Cumulative, source.Name, term.Slot);
            }
            break;

        case EMetricKind::Histogram:
            if (source.Wrapper == ESourceWrapper::Hist) {
                return BindHistogramAggregateTerm(spec, source, counters, term);
            }
            if (source.Wrapper == ESourceWrapper::None) {
                return BindPercentileTerm(spec, source, counters, term);
            }
            break;
        }

        AddProblem(spec, source, "is written in a form the metric kind does not allow");
        return false;
    }

    bool BindHistogramAggregateTerm(
        const TMetricSpec& spec,
        const TSourceRef& source,
        const TTabletCountersBase& counters,
        TBoundTerm& term)
    {
        ui32 histSlot;
        if (!Find(spec, source, counters, ECounterArray::Percentile, TString::Join("HIST(", source.Name, ")"), histSlot)) {
            return false;
        }

        const auto& percentile = counters.Percentile()[histSlot];
        if (!CheckBuckets(spec, source, percentile)) {
            return false;
        }

        term.Op = ESourceOp::HistOfSimple;
        term.Slot = FindCounter(counters, ECounterArray::Simple, source.Name);
        if (term.Slot == NO_SLOT) {
            term.Op = ESourceOp::HistOfCumulative;
            if (!Find(spec, source, counters, ECounterArray::Cumulative, source.Name, term.Slot)) {
                return false;
            }
        }

        for (ui32 i = 0; i + 1 < percentile.GetRangeCount(); ++i) {
            term.SourceBounds.push_back(static_cast<double>(percentile.GetRangeBound(i)));
        }
        term.SourceBounds.push_back(Max<double>());
        return true;
    }

    bool BindPercentileTerm(
        const TMetricSpec& spec,
        const TSourceRef& source,
        const TTabletCountersBase& counters,
        TBoundTerm& term)
    {
        if (!Find(spec, source, counters, ECounterArray::Percentile, source.Name, term.Slot)) {
            return false;
        }

        const auto& percentile = counters.Percentile()[term.Slot];
        if (!CheckBuckets(spec, source, percentile)) {
            return false;
        }

        if (percentile.GetIntegral() != spec.IsLevel) {
            AddProblem(spec, source, spec.IsLevel
                ? "is a derivative percentile counter, but the histogram holds a level"
                : "is an integral percentile counter, but the histogram holds increments");
            return false;
        }

        term.Op = spec.IsLevel ? ESourceOp::PercentileLevel : ESourceOp::PercentileIncrement;
        return true;
    }

    bool Find(
        const TMetricSpec& spec,
        const TSourceRef& source,
        const TTabletCountersBase& counters,
        ECounterArray array,
        TStringBuf name,
        ui32& slot)
    {
        static constexpr TStringBuf arrayNames[] = {"simple", "cumulative", "percentile"};

        slot = FindCounter(counters, array, name);
        if (slot == NO_SLOT) {
            AddProblem(spec, source, TStringBuilder()
                << "is missing: no " << arrayNames[static_cast<int>(array)] << " counter '" << name << "'");
            return false;
        }
        return true;
    }

    /**
     * An uninitialized percentile counter is dropped. Another bucket count is kept, but reported:
     * the values are clamped into the public buckets.
     */
    bool CheckBuckets(const TMetricSpec& spec, const TSourceRef& source, const TTabletPercentileCounter& percentile) {
        const ui32 rangeCount = percentile.GetRangeCount();

        if (rangeCount < 2) {
            AddProblem(spec, source, TStringBuilder()
                << "has an uninitialized percentile counter (" << rangeCount << " buckets)");
            return false;
        }

        if (rangeCount != spec.BucketCount()) {
            AddProblem(spec, source, TStringBuilder()
                << "has " << rangeCount << " buckets, but the histogram has " << spec.BucketCount() << " (kept)");
        }
        return true;
    }

    void AllocateState(const TMetricSpec& spec, TBoundTerm& term) {
        ui32 size = 0;

        switch (term.Op) {
        case ESourceOp::SimpleSum:
        case ESourceOp::SimpleMax:
        case ESourceOp::HistOfSimple:
        case ESourceOp::HistOfCumulative:
            size = 1;
            break;
        case ESourceOp::PercentileLevel:
            size = spec.BucketCount();
            break;
        case ESourceOp::CumulativeDelta:
        case ESourceOp::PercentileIncrement:
            return;
        }

        term.StateOffset = Binding.PerSourceStateSize;
        Binding.PerSourceStateSize += size;
    }

    void AddProblem(const TMetricSpec& spec, const TSourceRef& source, const TString& reason) {
        Binding.Problems.push_back(TStringBuilder()
            << "metric '" << spec.Name << "': source counter '" << source.Name << "' " << reason);
    }

private:
    const TTabletCountersBase& ExecutorCounters;
    const TTabletCountersBase& AppCounters;
    TDetailedMetricsBinding& Binding;
};

} // namespace

TMetricSpec MakeMetricSpec(
    EMetricKind kind,
    const TString& name,
    bool leaderOnly,
    TVector<TSourceRef> sources,
    TVector<ui64> bounds,
    bool integral)
{
    const auto count = [&sources](ESourceWrapper wrapper) {
        return std::count_if(sources.begin(), sources.end(), [wrapper](const auto& source) {
            return source.Wrapper == wrapper;
        });
    };

    TMetricSpec spec;
    spec.Name = name;
    spec.PartitionName = MakeYdbMetricName(name, EYdbMetricNameScope::Partition);
    spec.LeaderOnly = leaderOnly;
    spec.CombineByMax = kind == EMetricKind::Gauge && !sources.empty() && count(ESourceWrapper::Max) == std::ssize(sources);
    spec.StaticLevel = kind == EMetricKind::Histogram && !sources.empty() && count(ESourceWrapper::Hist) == std::ssize(sources);
    spec.IsLevel = kind == EMetricKind::Histogram && (count(ESourceWrapper::Hist) > 0 || integral);
    spec.Sources = std::move(sources);
    spec.Bounds = std::move(bounds);
    return spec;
}

const TDetailedMetricsDescriptor* GetDetailedMetricsDescriptor(TTabletTypes::EType tabletType) {
    switch (tabletType) {
    case TTabletTypes::DataShard: {
        static const TDetailedMetricsDescriptor descriptor = BuildDescriptor<
            NDataShard::ESimpleDetailedCounters_descriptor,
            NDataShard::ECumulativeDetailedCounters_descriptor,
            NDataShard::EPercentileDetailedCounters_descriptor
        >(TTabletTypes::DataShard);
        return &descriptor;
    }

    default:
        return nullptr;
    }
}

TDetailedMetricsBinding::TLayoutSizes TDetailedMetricsBinding::GetLayoutSizes(
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters)
{
    return {
        executorCounters.Simple().Size(),
        executorCounters.Cumulative().Size(),
        executorCounters.Percentile().Size(),
        appCounters.Simple().Size(),
        appCounters.Cumulative().Size(),
        appCounters.Percentile().Size(),
    };
}

THolder<TDetailedMetricsBinding> BindDetailedMetrics(
    const TDetailedMetricsDescriptor& descriptor,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters)
{
    auto binding = MakeHolder<TDetailedMetricsBinding>();
    TBinder(descriptor, executorCounters, appCounters, *binding);
    return binding;
}

} // namespace NKikimr
