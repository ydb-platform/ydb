#pragma once

#include "detailed_metrics_counter_set.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/counters.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <optional>

namespace NKikimr {

/**
 * The wrapper around the name of a source counter in the SourceCounters
 * definition of a public detailed metric.
 */
enum class ESourceWrapper {
    /**
     * A plain name "x": the cumulative or the percentile counter x itself.
     */
    None,
    /**
     * "SUM(x)": a gauge, which sums the simple counter x over all source tablets.
     */
    Sum,
    /**
     * "MAX(x)": a gauge, which takes the maximum of the simple counter x
     * over all source tablets.
     */
    Max,
    /**
     * "HIST(x)": a histogram of the current values (or the rates) of the counter x
     * over all source tablets.
     */
    Hist,
};

/**
 * A reference to one source counter of a public detailed metric.
 */
struct TSourceRef {
    ESourceCounterCategory Category = ESourceCounterCategory::SCC_TABLET;
    ESourceWrapper Wrapper = ESourceWrapper::None;

    /**
     * The name of the source counter x with the wrapper stripped
     * (for example, "ConsumedCPU" for "HIST(ConsumedCPU)").
     */
    TString Name;

    bool operator==(const TSourceRef& other) const = default;
};

/**
 * Parse the name of a source counter: "SUM(x)", "MAX(x)", "HIST(x)" or a plain "x".
 *
 * @note A plain name may contain parentheses (for example, "Tx(all)"),
 *       but a name, which starts with a wrapper, must be exactly one wrapper
 *       around a non-empty name.
 *
 * @param[in] text The name as written in the SourceCounters definition
 * @param[in] category The category of the source counter
 *
 * @return The parsed reference, or std::nullopt if the name is malformed
 */
std::optional<TSourceRef> ParseSourceRef(TStringBuf text, ESourceCounterCategory category);

/**
 * Format the source counter name back the way it is written
 * in the SourceCounters definition (for example, "HIST(ConsumedCPU)").
 */
TString FormatSourceRef(const TSourceRef& source);

/**
 * The kind of a public detailed metric, which decides the rules for its sources.
 */
enum class EMetricKind {
    /**
     * A gauge (the ESimpleDetailedCounters enum).
     */
    Gauge,
    /**
     * A rate (the ECumulativeDetailedCounters enum).
     */
    Rate,
    /**
     * A histogram (the EPercentileDetailedCounters enum).
     */
    Histogram,
};

/**
 * @return The lower case name of the metric kind (for example, "gauge")
 */
TStringBuf GetMetricKindName(EMetricKind kind);

/**
 * The specification of one public detailed metric.
 */
struct TMetricSpec {
    /**
     * The aggregate name of the metric, as defined in the .proto file.
     */
    TString Name;

    /**
     * The partition-level leaf name: MakeYdbMetricName(Name, EYdbMetricNameScope::Partition),
     * built once and shared by every leaf target.
     */
    TString PartitionName;

    /**
     * Whether the metric is meaningful only on leaders (followers publish nothing for it).
     */
    bool LeaderOnly = false;

    /**
     * The source counters, whose values are combined into the metric value.
     *
     * @note Empty for a metric, which failed the validation: such a metric
     *       keeps its targets, but always publishes zero.
     */
    TVector<TSourceRef> Sources;

    /**
     * Gauges only: every source is MAX(x), so partial values (for example,
     * those reported by different nodes) are combined by MAX instead of SUM.
     */
    bool CombineByMax = false;

    /**
     * Histograms only: the public bucket bounds (+Inf is implicit).
     */
    TVector<ui64> Bounds;

    /**
     * Histograms only: every source is HIST(x), so the histogram describes
     * the current state (a level) of the source tablets rather than increments.
     */
    bool StaticLevel = false;

    /**
     * Histograms only: the Integral option (CounterOpts) of the enum entry of the metric,
     * which declares a histogram over plain percentile sources a level.
     */
    bool Integral = false;

    /**
     * Histograms only: the histogram holds a level (the current state of the source
     * tablets, which goes up and down) rather than increments, which only accumulate:
     * any source is HIST(x), or the metric is Integral.
     *
     * @note Every source must agree: HIST(x) and an integral percentile counter are
     *       levels, a derivative (non-integral) percentile counter is increments.
     *       The kind of a plain percentile counter is known only from the counter layout,
     *       so a source, which disagrees, is rejected when the metrics are bound
     *       to the layout (see TDetailedMetricsBinding), not here.
     */
    bool IsLevel = false;

    /**
     * Histograms only: the number of buckets, including the implicit +Inf one.
     */
    size_t BucketCount() const {
        return Bounds.size() + 1;
    }
};

/**
 * The static description of all public detailed metrics of one tablet type,
 * as defined in the corresponding counters_detailed_<type>.proto file.
 *
 * @warning The index of a metric in Gauges, Rates and Histograms is the value
 *          of its enum entry in the ESimpleDetailedCounters, ECumulativeDetailedCounters
 *          and EPercentileDetailedCounters enums, and it is also the slot of the metric
 *          on the wire between nodes and the SysView Processor. Public enum entries are
 *          therefore append-only: an entry is never removed or renumbered, a new metric
 *          is always a new entry at the end of its enum.
 */
struct TDetailedMetricsDescriptor {
    TTabletTypes::EType Type = TTabletTypes::TypeInvalid;

    /**
     * The public metrics by kind, the index is the enum value (the wire slot).
     */
    TVector<TMetricSpec> Gauges;
    TVector<TMetricSpec> Rates;
    TVector<TMetricSpec> Histograms;

    /**
     * The allow-list of the low level counters for the TABLE raw tree:
     * every source name as written, plus x for SUM(x), MAX(x) and HIST(x).
     */
    TDetailedMetricsCounterNames RawNames;

    /**
     * The errors found when the descriptor was built. The descriptor is static
     * (built without an actor context), so the users log these errors themselves.
     */
    TVector<TString> Errors;

    /**
     * @return The public metrics of the given kind (Gauges, Rates or Histograms)
     */
    const TVector<TMetricSpec>& GetMetrics(EMetricKind kind) const;
};

/**
 * Validate the descriptor and fill the derived fields: PartitionName, CombineByMax,
 * StaticLevel, IsLevel and RawNames.
 *
 * Validation rules:
 * - a metric name has at least three segments (as MakeYdbMetricName requires),
 *   metric names are unique and every metric has at least one source;
 * - a gauge source is SUM(x) or MAX(x), all sources of a gauge use one wrapper,
 *   and a MAX(x) gauge has exactly one source;
 * - a rate source is a plain name;
 * - a histogram source is HIST(x) or a plain percentile name, and the histogram
 *   has strictly increasing bounds.
 *
 * A metric, which fails the validation, keeps its specification (so its targets are
 * still created), but loses its sources (so it always publishes zero).
 * Every error is appended to descriptor.Errors.
 *
 * @param[in,out] descriptor The descriptor to validate and finalize
 * @param[out] error If not null, receives all errors of the descriptor joined together
 *
 * @return Whether the descriptor has no errors
 */
bool FinalizeDescriptor(TDetailedMetricsDescriptor& descriptor, TString* error);

namespace NDetailedMetricsDescriptorImpl {

/**
 * Append the specifications of all metrics of one kind, parsed from the given
 * enum options, to the given list.
 *
 * @note withBounds is set for histograms only: it also takes their Integral option.
 */
template <const NProtoBuf::EnumDescriptor* Desc()>
void AppendMetricSpecs(TVector<TMetricSpec>& specs, TVector<TString>& errors, bool withBounds) {
    const auto* opts = NAux::GetAppOpts<Desc, true /* ParseSourceCounters */>();
    const NProtoBuf::EnumDescriptor* enumDesc = Desc();
    const auto& globalRanges = enumDesc->options().GetExtension(GlobalCounterOpts).GetRanges();

    specs.reserve(opts->Size);

    for (size_t i = 0; i < opts->Size; ++i) {
        auto& spec = specs.emplace_back();
        spec.Name = opts->GetNames()[i];
        spec.LeaderOnly = opts->GetLeaderOnly(i);
        spec.Integral = withBounds && opts->GetIntegral(i);

        for (const auto& source : opts->GetSourceCounters(i)) {
            auto ref = ParseSourceRef(source.GetName(), source.GetCategory());

            if (!ref) {
                errors.push_back(TStringBuilder()
                    << "metric '" << spec.Name << "': malformed source counter '" << source.GetName() << "'");
                spec.Sources.clear();
                break;
            }

            spec.Sources.push_back(std::move(*ref));
        }

        // NOTE: TAppParsedOpts::GetRanges() aborts if the ranges are not defined,
        //       so check them first and leave the bounds empty for FinalizeDescriptor()
        const auto& ranges = enumDesc->value(i)->options().GetExtension(CounterOpts).GetRanges();

        if (withBounds && (!ranges.empty() || !globalRanges.empty())) {
            for (const auto& range : opts->GetRanges(i)) {
                spec.Bounds.push_back(range.RangeVal);
            }
        }
    }
}

} // namespace NDetailedMetricsDescriptorImpl

/**
 * Build the descriptor from the enums of a counters_detailed_<type>.proto file,
 * parsed by NAux::GetAppOpts<Desc, true>() the same way as for the YDB metrics mapper.
 *
 * @note The returned descriptor is not finalized yet, see FinalizeDescriptor().
 *
 * @tparam SimpleDesc The function, which returns the enum description for gauges
 * @tparam CumulativeDesc The function, which returns the enum description for rates
 * @tparam PercentileDesc The function, which returns the enum description for histograms
 *
 * @param[in] type The tablet type, which the descriptor is built for
 *
 * @return The descriptor, which still needs to be finalized
 */
template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc()>
TDetailedMetricsDescriptor BuildDescriptor(TTabletTypes::EType type) {
    TDetailedMetricsDescriptor descriptor;
    descriptor.Type = type;

    NDetailedMetricsDescriptorImpl::AppendMetricSpecs<SimpleDesc>(
        descriptor.Gauges, descriptor.Errors, false /* withBounds */);
    NDetailedMetricsDescriptorImpl::AppendMetricSpecs<CumulativeDesc>(
        descriptor.Rates, descriptor.Errors, false /* withBounds */);
    NDetailedMetricsDescriptorImpl::AppendMetricSpecs<PercentileDesc>(
        descriptor.Histograms, descriptor.Errors, true /* withBounds */);

    return descriptor;
}

/**
 * Get the static descriptor of the public detailed metrics of the given tablet type.
 *
 * @param[in] tabletType The tablet type
 *
 * @return The descriptor, or nullptr if the tablet type has no detailed metrics
 */
const TDetailedMetricsDescriptor* GetDetailedMetricsDescriptor(TTabletTypes::EType tabletType);

} // namespace NKikimr
