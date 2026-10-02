#include "detailed_metrics_binding.h"

#include <util/digest/numeric.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>

namespace NKikimr {

namespace {

constexpr ui32 NO_SLOT = TBoundTerm::NoSlot;

/**
 * The counter arrays of one bank.
 */
enum class ECounterArray {
    Simple,
    Cumulative,
    Percentile,
};

/**
 * @return The bank of the given source counter category: the same rule as the YDB
 *         metrics mapper uses, anything but SCC_TABLET is an Executor counter
 */
EBank GetBank(ESourceCounterCategory category) {
    return category == ESourceCounterCategory::SCC_TABLET ? EBank::App : EBank::Executor;
}

TStringBuf GetBankName(EBank bank) {
    switch (bank) {
    case EBank::Executor:
        return "executor";
    case EBank::App:
        return "app";
    }

    Y_ABORT("unexpected bank %d", static_cast<int>(bank));
}

const TTabletCountersBase& SelectBank(
    EBank bank,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters)
{
    return bank == EBank::Executor ? executorCounters : appCounters;
}

/**
 * @return The counter array, where the Slot of a term with the given operation lives
 */
ECounterArray GetSlotArray(ESourceOp op) {
    switch (op) {
    case ESourceOp::SimpleSum:
    case ESourceOp::SimpleMax:
    case ESourceOp::HistOfSimple:
        return ECounterArray::Simple;
    case ESourceOp::CumulativeDelta:
    case ESourceOp::HistOfCumulative:
        return ECounterArray::Cumulative;
    case ESourceOp::PercentileLevel:
    case ESourceOp::PercentileIncrement:
        return ECounterArray::Percentile;
    }

    Y_ABORT("unexpected source operation %d", static_cast<int>(op));
}

ui32 GetArraySize(const TTabletCountersBase& counters, ECounterArray array) {
    switch (array) {
    case ECounterArray::Simple:
        return counters.Simple().Size();
    case ECounterArray::Cumulative:
        return counters.Cumulative().Size();
    case ECounterArray::Percentile:
        return counters.Percentile().Size();
    }

    Y_ABORT("unexpected counter array %d", static_cast<int>(array));
}

/**
 * @return The name of the counter at the given slot, or nullptr if the slot
 *         is out of range or has no name
 */
const char* GetCounterName(const TTabletCountersBase& counters, ECounterArray array, ui32 slot) {
    if (slot >= GetArraySize(counters, array)) {
        return nullptr;
    }

    switch (array) {
    case ECounterArray::Simple:
        return counters.SimpleCounterName(slot);
    case ECounterArray::Cumulative:
        return counters.CumulativeCounterName(slot);
    case ECounterArray::Percentile:
        return counters.PercentileCounterName(slot);
    }

    Y_ABORT("unexpected counter array %d", static_cast<int>(array));
}

/**
 * @return The first slot of the array, whose counter has the given name, or NO_SLOT
 */
ui32 FindCounter(const TTabletCountersBase& counters, ECounterArray array, TStringBuf name) {
    const ui32 size = GetArraySize(counters, array);

    for (ui32 slot = 0; slot < size; ++slot) {
        const char* counterName = GetCounterName(counters, array, slot);

        if (counterName && name == counterName) {
            return slot;
        }
    }

    return NO_SLOT;
}

/**
 * @return Whether the given name is exactly "HIST(<base>)"
 */
bool IsHistogramAggregateOf(const char* name, TStringBuf base) {
    if (!name) {
        return false;
    }

    constexpr TStringBuf prefix = "HIST(";
    const TStringBuf text(name);

    return text.size() == prefix.size() + base.size() + 1
        && text.StartsWith(prefix)
        && text.EndsWith(')')
        && text.SubStr(prefix.size(), base.size()) == base;
}

/**
 * Resolves every source counter of a descriptor in one counter layout.
 */
class TBinder {
public:
    TBinder(
        const TDetailedMetricsDescriptor& descriptor,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TDetailedMetricsBinding& binding)
        : Descriptor(descriptor)
        , ExecutorCounters(executorCounters)
        , AppCounters(appCounters)
        , Binding(binding)
    {
    }

    void Bind() {
        Binding.Descriptor = &Descriptor;
        Binding.LayoutSizes = TDetailedMetricsBinding::GetLayoutSizes(ExecutorCounters, AppCounters);

        BindMetrics(EMetricKind::Gauge);
        BindMetrics(EMetricKind::Rate);
        BindMetrics(EMetricKind::Histogram);

        Binding.HistIsLevel.reserve(Descriptor.Histograms.size());
        for (const auto& spec : Descriptor.Histograms) {
            Binding.HistIsLevel.push_back(spec.IsLevel);
        }

        Binding.LayoutSignature = Binding.GetLayoutSignature(ExecutorCounters, AppCounters);
    }

private:
    void BindMetrics(EMetricKind kind) {
        const auto& specs = Descriptor.GetMetrics(kind);

        for (ui32 metric = 0; metric < specs.size(); ++metric) {
            const auto& spec = specs[metric];

            // The pending increment buckets of the metric, shared by all its terms
            ui32 pendingOffset = NO_SLOT;

            for (ui32 source = 0; source < spec.Sources.size(); ++source) {
                TBoundTerm term;
                term.Kind = kind;
                term.Metric = metric;
                term.Source = source;
                term.Bank = GetBank(spec.Sources[source].Category);
                term.LeaderOnly = spec.LeaderOnly;

                if (!BindTerm(spec, spec.Sources[source], term)) {
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

    /**
     * Resolve one source counter.
     *
     * @return Whether the source counter is bound (otherwise, a problem is reported)
     */
    bool BindTerm(const TMetricSpec& spec, const TSourceRef& source, TBoundTerm& term) {
        switch (term.Kind) {
        case EMetricKind::Gauge:
            if (source.Wrapper == ESourceWrapper::Sum || source.Wrapper == ESourceWrapper::Max) {
                return BindGaugeTerm(spec, source, term);
            }

            break;

        case EMetricKind::Rate:
            if (source.Wrapper == ESourceWrapper::None) {
                return BindRateTerm(spec, source, term);
            }

            break;

        case EMetricKind::Histogram:
            if (source.Wrapper == ESourceWrapper::Hist) {
                return BindHistogramAggregateTerm(spec, source, term);
            }

            if (source.Wrapper == ESourceWrapper::None) {
                return BindPercentileTerm(spec, source, term);
            }

            break;
        }

        // Not possible for a finalized descriptor, which validates the wrappers
        AddProblem(spec, source, term, TStringBuilder()
            << "is not allowed for a " << GetMetricKindName(term.Kind));
        return false;
    }

    /**
     * SUM(x) or MAX(x): x is a simple counter.
     */
    bool BindGaugeTerm(const TMetricSpec& spec, const TSourceRef& source, TBoundTerm& term) {
        const auto& counters = GetBankCounters(term.Bank);
        const ui32 slot = FindCounter(counters, ECounterArray::Simple, source.Name);

        if (slot == NO_SLOT) {
            if (FindCounter(counters, ECounterArray::Cumulative, source.Name) != NO_SLOT) {
                AddProblem(spec, source, term, TStringBuilder()
                    << "needs a simple counter, but '" << source.Name << "' is a cumulative counter");
            } else {
                AddProblem(spec, source, term, TStringBuilder()
                    << "is missing: no simple counter '" << source.Name << "'");
            }

            return false;
        }

        term.Op = source.Wrapper == ESourceWrapper::Max ? ESourceOp::SimpleMax : ESourceOp::SimpleSum;
        term.Slot = slot;
        return true;
    }

    /**
     * A rate source x: x is a cumulative counter.
     */
    bool BindRateTerm(const TMetricSpec& spec, const TSourceRef& source, TBoundTerm& term) {
        const auto& counters = GetBankCounters(term.Bank);
        const ui32 slot = FindCounter(counters, ECounterArray::Cumulative, source.Name);

        if (slot == NO_SLOT) {
            if (FindCounter(counters, ECounterArray::Simple, source.Name) != NO_SLOT) {
                AddProblem(spec, source, term, TStringBuilder()
                    << "needs a cumulative counter, but '" << source.Name << "' is a simple counter");
            } else {
                AddProblem(spec, source, term, TStringBuilder()
                    << "is missing: no cumulative counter '" << source.Name << "'");
            }

            return false;
        }

        term.Op = ESourceOp::CumulativeDelta;
        term.Slot = slot;
        return true;
    }

    /**
     * HIST(x): the percentile counter HIST(x) defines the buckets, the base counter x
     * (simple first, then cumulative) gives one observation per source.
     */
    bool BindHistogramAggregateTerm(const TMetricSpec& spec, const TSourceRef& source, TBoundTerm& term) {
        if (!spec.IsLevel) {
            AddProblem(spec, source, term, "is a level, but the histogram holds increments");
            return false;
        }

        const auto& counters = GetBankCounters(term.Bank);
        const TString histName = FormatSourceRef(source);
        const ui32 histSlot = FindCounter(counters, ECounterArray::Percentile, histName);

        if (histSlot == NO_SLOT) {
            AddProblem(spec, source, term, TStringBuilder()
                << "is missing: no percentile counter '" << histName << "'");
            return false;
        }

        const auto& percentile = counters.Percentile()[histSlot];
        const ui32 rangeCount = percentile.GetRangeCount();

        if (rangeCount < 2) {
            AddProblem(spec, source, term, TStringBuilder()
                << "has an uninitialized percentile counter '" << histName
                << "' (" << rangeCount << " buckets)");
            return false;
        }

        // The same order as NPrivate::TAggregatedTabletCounters::Initialize():
        // a simple counter x takes the histogram aggregate before a cumulative one
        ui32 baseSlot = FindCounter(counters, ECounterArray::Simple, source.Name);
        term.Op = ESourceOp::HistOfSimple;

        if (baseSlot == NO_SLOT) {
            baseSlot = FindCounter(counters, ECounterArray::Cumulative, source.Name);
            term.Op = ESourceOp::HistOfCumulative;
        }

        if (baseSlot == NO_SLOT) {
            AddProblem(spec, source, term, TStringBuilder()
                << "has no base counter '" << source.Name << "' (neither simple nor cumulative)");
            return false;
        }

        term.Slot = baseSlot;
        term.HistSlot = histSlot;

        // The +Inf bucket is implicit in the percentile counter, but explicit here
        term.SourceBounds.reserve(rangeCount);
        for (ui32 i = 0; i + 1 < rangeCount; ++i) {
            term.SourceBounds.push_back(static_cast<double>(percentile.GetRangeBound(i)));
        }
        term.SourceBounds.push_back(Max<double>());

        CheckBucketCount(spec, source, term, rangeCount);
        return true;
    }

    /**
     * A plain histogram source p: p is a percentile counter, integral for a level histogram
     * and derivative for an increment histogram.
     */
    bool BindPercentileTerm(const TMetricSpec& spec, const TSourceRef& source, TBoundTerm& term) {
        const auto& counters = GetBankCounters(term.Bank);
        const ui32 slot = FindCounter(counters, ECounterArray::Percentile, source.Name);

        if (slot == NO_SLOT) {
            AddProblem(spec, source, term, TStringBuilder()
                << "is missing: no percentile counter '" << source.Name << "'");
            return false;
        }

        const auto& percentile = counters.Percentile()[slot];
        const ui32 rangeCount = percentile.GetRangeCount();

        if (rangeCount < 2) {
            AddProblem(spec, source, term, TStringBuilder()
                << "is an uninitialized percentile counter (" << rangeCount << " buckets)");
            return false;
        }

        const bool integral = percentile.GetIntegral();

        if (integral != spec.IsLevel) {
            AddProblem(spec, source, term, integral
                ? "is an integral percentile counter (a level), but the histogram holds increments"
                : "is a derivative percentile counter (increments), but the histogram holds a level");
            return false;
        }

        term.Op = integral ? ESourceOp::PercentileLevel : ESourceOp::PercentileIncrement;
        term.Slot = slot;

        CheckBucketCount(spec, source, term, rangeCount);
        return true;
    }

    /**
     * Report a source bucket count, which differs from the public one. The term is kept:
     * the values are clamped into the public buckets.
     */
    void CheckBucketCount(const TMetricSpec& spec, const TSourceRef& source, const TBoundTerm& term, ui32 rangeCount) {
        if (rangeCount != spec.BucketCount()) {
            AddProblem(spec, source, term, TStringBuilder()
                << "has " << rangeCount << " buckets, but the histogram has " << spec.BucketCount()
                << " (kept, the extra source buckets are counted in the last public bucket)");
        }
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

    void AddProblem(const TMetricSpec& spec, const TSourceRef& source, const TBoundTerm& term, const TString& reason) {
        Binding.Problems.push_back(TStringBuilder()
            << GetMetricKindName(term.Kind) << " '" << spec.Name << "': source counter '"
            << GetBankName(term.Bank) << ":" << FormatSourceRef(source) << "' " << reason);
    }

    const TTabletCountersBase& GetBankCounters(EBank bank) const {
        return SelectBank(bank, ExecutorCounters, AppCounters);
    }

private:
    const TDetailedMetricsDescriptor& Descriptor;
    const TTabletCountersBase& ExecutorCounters;
    const TTabletCountersBase& AppCounters;
    TDetailedMetricsBinding& Binding;
};

} // namespace

size_t TDetailedMetricsLayoutSignature::Hash() const noexcept {
    ui64 hash = NamesHash;

    for (const ui32 size : Sizes) {
        hash = CombineHashes<ui64>(hash, IntHash<ui64>(size));
    }

    return hash;
}

TDetailedMetricsBinding::TLayoutSizes TDetailedMetricsBinding::GetLayoutSizes(
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters)
{
    TLayoutSizes sizes;
    sizes[ExecutorSimple] = executorCounters.Simple().Size();
    sizes[ExecutorCumulative] = executorCounters.Cumulative().Size();
    sizes[ExecutorPercentile] = executorCounters.Percentile().Size();
    sizes[AppSimple] = appCounters.Simple().Size();
    sizes[AppCumulative] = appCounters.Cumulative().Size();
    sizes[AppPercentile] = appCounters.Percentile().Size();
    return sizes;
}

const TSourceRef& TDetailedMetricsBinding::GetSource(const TBoundTerm& term) const {
    Y_ABORT_UNLESS(Descriptor);

    const auto& specs = Descriptor->GetMetrics(term.Kind);
    Y_ABORT_UNLESS(term.Metric < specs.size());

    const auto& sources = specs[term.Metric].Sources;
    Y_ABORT_UNLESS(term.Source < sources.size());

    return sources[term.Source];
}

bool TDetailedMetricsBinding::Matches(
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters) const
{
    if (GetLayoutSizes(executorCounters, appCounters) != LayoutSizes) {
        return false;
    }

    for (const auto& term : Terms) {
        const auto& counters = SelectBank(term.Bank, executorCounters, appCounters);
        const auto& source = GetSource(term);

        const char* name = GetCounterName(counters, GetSlotArray(term.Op), term.Slot);
        if (!name || source.Name != name) {
            return false;
        }

        if (term.HistSlot != NO_SLOT
            && !IsHistogramAggregateOf(GetCounterName(counters, ECounterArray::Percentile, term.HistSlot), source.Name))
        {
            return false;
        }
    }

    return true;
}

TDetailedMetricsLayoutSignature TDetailedMetricsBinding::GetLayoutSignature(
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters) const
{
    TDetailedMetricsLayoutSignature signature;
    signature.Sizes = GetLayoutSizes(executorCounters, appCounters);

    ui64 hash = 0;
    const auto addName = [&hash](const char* name) {
        // A slot out of range or without a name is hashed as a fixed value
        hash = CombineHashes<ui64>(hash, name ? THash<TStringBuf>()(name) : 0);
    };

    for (const auto& term : Terms) {
        const auto& counters = SelectBank(term.Bank, executorCounters, appCounters);
        addName(GetCounterName(counters, GetSlotArray(term.Op), term.Slot));

        if (term.HistSlot != NO_SLOT) {
            addName(GetCounterName(counters, ECounterArray::Percentile, term.HistSlot));
        }
    }

    signature.NamesHash = hash;
    return signature;
}

THolder<TDetailedMetricsBinding> BindDetailedMetrics(
    const TDetailedMetricsDescriptor& descriptor,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters)
{
    auto binding = MakeHolder<TDetailedMetricsBinding>();
    TBinder(descriptor, executorCounters, appCounters, *binding).Bind();
    return binding;
}

} // namespace NKikimr
