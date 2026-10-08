#include "detailed_metrics_binding.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr {

namespace {

constexpr ui32 NO_SLOT = TBoundTerm::NoSlot;

// Parsed as for the YDB metrics mapper: a malformed definition aborts there
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
        bool nonDerivative = false;
        if (kind == EMetricKind::Histogram) {
            for (const auto& range : opts->GetRanges(i)) {
                bounds.push_back(range.RangeVal);
            }
            nonDerivative = opts->GetIntegral(i)
                || std::any_of(sources.begin(), sources.end(), [](const TSourceRef& source) {
                    return source.Wrapper == ESourceWrapper::Hist;
                });
        }

        specs.push_back(TMetricSpec{
            .Name = opts->GetNames()[i],
            .LeaderOnly = opts->GetLeaderOnly(i),
            .Sources = std::move(sources),
            .Bounds = std::move(bounds),
            .NonDerivative = nonDerivative,
        });
    }

    return specs;
}

void AddCounterNames(TDetailedMetricsDescriptor& descriptor, const TSourceRef& source) {
    auto& names = source.Category == ESourceCounterCategory::SCC_TABLET
        ? descriptor.AppCounterNames
        : descriptor.ExecutorCounterNames;

    names.insert(source.Text);
    if (source.Wrapper == ESourceWrapper::Sum || source.Wrapper == ESourceWrapper::Max) {
        names.insert(source.Name);
    }
}

template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc()>
TDetailedMetricsDescriptor BuildDescriptor(TTabletTypes::EType type) {
    TDetailedMetricsDescriptor descriptor{
        .Type = type,
        .Gauges = BuildMetricSpecs<SimpleDesc>(EMetricKind::Gauge),
        .Rates = BuildMetricSpecs<CumulativeDesc>(EMetricKind::Rate),
        .Histograms = BuildMetricSpecs<PercentileDesc>(EMetricKind::Histogram),
    };

    for (const auto* specs : {&descriptor.Gauges, &descriptor.Rates, &descriptor.Histograms}) {
        for (const auto& spec : *specs) {
            for (const auto& source : spec.Sources) {
                AddCounterNames(descriptor, source);
            }
        }
    }

    return descriptor;
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
                term.Category = source.Category;
                term.LeaderOnly = spec.LeaderOnly;

                if (!BindTerm(spec, source, term)) {
                    continue;
                }

                AllocateState(spec, term);

                if (term.Op == ESourceOp::PercentileDerivative) {
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
        const auto& counters = term.Category == ESourceCounterCategory::SCC_TABLET ? AppCounters : ExecutorCounters;

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
        if (!Find(spec, source, counters, ECounterArray::Percentile, source.Text, histSlot)) {
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

        const bool nonDerivative = spec.NonDerivative;
        if (percentile.GetIntegral() != nonDerivative) {
            AddProblem(spec, source, nonDerivative
                ? "is a derivative percentile counter, but the histogram is non-derivative"
                : "is an integral percentile counter, but the histogram is derivative");
            return false;
        }

        term.Op = nonDerivative ? ESourceOp::PercentileNonDerivative : ESourceOp::PercentileDerivative;
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

    // Another bucket count is kept: the values are clamped into the public buckets
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
        case ESourceOp::PercentileNonDerivative:
            size = spec.BucketCount();
            break;
        case ESourceOp::CumulativeDelta:
        case ESourceOp::PercentileDerivative:
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

TSourceRef ParseSourceRef(TStringBuf text, ESourceCounterCategory category) {
    static constexpr std::pair<TStringBuf, ESourceWrapper> wrappers[] = {
        {"SUM(", ESourceWrapper::Sum},
        {"MAX(", ESourceWrapper::Max},
        {"HIST(", ESourceWrapper::Hist},
    };

    for (const auto& [prefix, wrapper] : wrappers) {
        TStringBuf name = text;
        if (name.SkipPrefix(prefix) && name.ChopSuffix(")")) {
            return TSourceRef{.Category = category, .Wrapper = wrapper, .Name = TString(name), .Text = TString(text)};
        }
    }

    return TSourceRef{.Category = category, .Wrapper = ESourceWrapper::None, .Name = TString(text), .Text = TString(text)};
}

bool TMetricSpec::CombineByMax() const {
    return !Sources.empty() && std::all_of(Sources.begin(), Sources.end(), [](const TSourceRef& source) {
        return source.Wrapper == ESourceWrapper::Max;
    });
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
