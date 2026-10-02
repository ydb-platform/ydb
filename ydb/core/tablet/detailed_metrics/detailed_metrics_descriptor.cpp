#include "detailed_metrics_descriptor.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/monlib/metrics/histogram_snapshot.h>

#include <util/generic/hash_set.h>
#include <util/string/join.h>

namespace NKikimr {

namespace {

/**
 * The prefixes of all wrapped source counter names.
 */
constexpr std::pair<TStringBuf, ESourceWrapper> SOURCE_WRAPPERS[] = {
    {TStringBuf("SUM("), ESourceWrapper::Sum},
    {TStringBuf("MAX("), ESourceWrapper::Max},
    {TStringBuf("HIST("), ESourceWrapper::Hist},
};

bool StartsWithWrapper(TStringBuf text) {
    for (const auto& [prefix, wrapper] : SOURCE_WRAPPERS) {
        if (text.StartsWith(prefix)) {
            return true;
        }
    }

    return false;
}

/**
 * Check that the metric name has at least three segments, the same way
 * as MakeYdbMetricName() does for the Partition scope.
 */
bool HasThreeSegments(TStringBuf name) {
    const size_t firstDot = name.find('.');

    if (firstDot == TStringBuf::npos) {
        return false;
    }

    const size_t secondDot = name.find('.', firstDot + 1);
    return secondDot != TStringBuf::npos && secondDot + 1 < name.size();
}

/**
 * The kind of a public metric, which decides the validation rules for its sources.
 */
enum class EMetricKind {
    Gauge,
    Rate,
    Histogram,
};

TStringBuf GetMetricKindName(EMetricKind kind) {
    switch (kind) {
    case EMetricKind::Gauge:
        return "gauge";
    case EMetricKind::Rate:
        return "rate";
    case EMetricKind::Histogram:
        return "histogram";
    }

    Y_ABORT("unexpected metric kind %d", static_cast<int>(kind));
}

/**
 * Validate the specification of one metric.
 *
 * @return The reason why the metric is invalid, or an empty string if it is valid
 */
TString ValidateMetric(EMetricKind kind, const TMetricSpec& spec) {
    if (!HasThreeSegments(spec.Name)) {
        return "the name has fewer than three segments";
    }

    if (spec.Sources.empty()) {
        return "no valid source counters";
    }

    for (const auto& source : spec.Sources) {
        if (source.Name.empty()) {
            return "empty source counter name";
        }
    }

    switch (kind) {
    case EMetricKind::Gauge: {
        const ESourceWrapper wrapper = spec.Sources.front().Wrapper;

        for (const auto& source : spec.Sources) {
            if (source.Wrapper != ESourceWrapper::Sum && source.Wrapper != ESourceWrapper::Max) {
                return TStringBuilder() << "gauge source counter '" << FormatSourceRef(source)
                    << "' is not SUM(x) or MAX(x)";
            }

            if (source.Wrapper != wrapper) {
                return "gauge source counters mix SUM(x) and MAX(x)";
            }
        }

        if (wrapper == ESourceWrapper::Max && spec.Sources.size() > 1) {
            return "MAX(x) gauge has more than one source counter";
        }

        break;
    }

    case EMetricKind::Rate:
        for (const auto& source : spec.Sources) {
            if (source.Wrapper != ESourceWrapper::None) {
                return TStringBuilder() << "rate source counter '" << FormatSourceRef(source)
                    << "' is not a plain name";
            }
        }

        break;

    case EMetricKind::Histogram:
        for (const auto& source : spec.Sources) {
            if (source.Wrapper != ESourceWrapper::Hist && source.Wrapper != ESourceWrapper::None) {
                return TStringBuilder() << "histogram source counter '" << FormatSourceRef(source)
                    << "' is not HIST(x) or a plain name";
            }
        }

        if (spec.Bounds.empty()) {
            return "the histogram has no bounds (Ranges)";
        }

        // NOTE: The public series is an explicit monlib histogram, which limits the bound count
        if (spec.Bounds.size() > NMonitoring::HISTOGRAM_MAX_BUCKETS_COUNT) {
            return TStringBuilder() << "the histogram has " << spec.Bounds.size()
                << " bounds, more than " << NMonitoring::HISTOGRAM_MAX_BUCKETS_COUNT;
        }

        for (size_t i = 1; i < spec.Bounds.size(); ++i) {
            if (spec.Bounds[i - 1] >= spec.Bounds[i]) {
                return "the histogram bounds are not strictly increasing";
            }
        }

        break;
    }

    return {};
}

/**
 * Validate the metrics of one kind and fill their derived fields.
 */
void FinalizeMetrics(
    EMetricKind kind,
    TVector<TMetricSpec>& specs,
    THashSet<TString>& allNames,
    TVector<TString>& errors)
{
    for (size_t i = 0; i < specs.size(); ++i) {
        auto& spec = specs[i];
        TString reason = ValidateMetric(kind, spec);

        if (reason.empty() && !allNames.insert(spec.Name).second) {
            reason = "the name is not unique";
        }

        if (!reason.empty()) {
            errors.push_back(TStringBuilder()
                << GetMetricKindName(kind) << " #" << i << " '" << spec.Name << "': " << reason);

            // Keep the metric (so its targets are still created), but drop its sources,
            // so it always publishes zero
            spec.Sources.clear();
        }

        // NOTE: MakeYdbMetricName() aborts on a name with fewer than three segments,
        //       such a (broken) metric keeps the aggregate name for its leaves too
        spec.PartitionName = HasThreeSegments(spec.Name)
            ? MakeYdbMetricName(spec.Name, EYdbMetricNameScope::Partition)
            : spec.Name;

        const auto allWrapped = [&spec](ESourceWrapper wrapper) {
            if (spec.Sources.empty()) {
                return false;
            }

            for (const auto& source : spec.Sources) {
                if (source.Wrapper != wrapper) {
                    return false;
                }
            }

            return true;
        };

        spec.CombineByMax = kind == EMetricKind::Gauge && allWrapped(ESourceWrapper::Max);
        spec.StaticLevel = kind == EMetricKind::Histogram && allWrapped(ESourceWrapper::Hist);
    }
}

/**
 * Collect the allow-list of the low level counters, which are used by the given metrics.
 */
void CollectRawNames(const TVector<TMetricSpec>& specs, TDetailedMetricsCounterNames& names) {
    for (const auto& spec : specs) {
        for (const auto& source : spec.Sources) {
            auto& target = source.Category == ESourceCounterCategory::SCC_TABLET
                ? names.AppNames
                : names.ExecutorNames;

            target.insert(FormatSourceRef(source));

            // The base counter x is needed both for SUM(x)/MAX(x) and for HIST(x)
            // (the HIST(x) aggregate is built over the values of x)
            if (source.Wrapper != ESourceWrapper::None) {
                target.insert(source.Name);
            }
        }
    }
}

/**
 * Build and finalize the descriptor for the given tablet type.
 */
template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc()>
TDetailedMetricsDescriptor BuildFinalizedDescriptor(TTabletTypes::EType type) {
    auto descriptor = BuildDescriptor<SimpleDesc, CumulativeDesc, PercentileDesc>(type);

    TString error;
    if (!FinalizeDescriptor(descriptor, &error)) {
        Y_DEBUG_ABORT("invalid detailed metrics descriptor for %s: %s",
            TTabletTypes::TypeToStr(type), error.c_str());
    }

    return descriptor;
}

const TDetailedMetricsDescriptor* GetDataShardDescriptor() {
    static const TDetailedMetricsDescriptor descriptor = BuildFinalizedDescriptor<
        NDataShard::ESimpleDetailedCounters_descriptor,
        NDataShard::ECumulativeDetailedCounters_descriptor,
        NDataShard::EPercentileDetailedCounters_descriptor
    >(TTabletTypes::DataShard);

    return &descriptor;
}

} // namespace

std::optional<TSourceRef> ParseSourceRef(TStringBuf text, ESourceCounterCategory category) {
    if (text.empty()) {
        return std::nullopt;
    }

    for (const auto& [prefix, wrapper] : SOURCE_WRAPPERS) {
        if (!text.StartsWith(prefix)) {
            continue;
        }

        if (!text.EndsWith(')')) {
            return std::nullopt;
        }

        const TStringBuf name = text.SubStr(prefix.size(), text.size() - prefix.size() - 1);

        // Exactly one wrapper around a non-empty name
        if (name.empty() || StartsWithWrapper(name)) {
            return std::nullopt;
        }

        // The HIST(x) aggregate is matched to its base counter x
        // by GetHistogramAggregateSimpleName(), so both must agree on x
        if (wrapper == ESourceWrapper::Hist && GetHistogramAggregateSimpleName(text) != name) {
            return std::nullopt;
        }

        return TSourceRef{category, wrapper, TString(name)};
    }

    return TSourceRef{category, ESourceWrapper::None, TString(text)};
}

TString FormatSourceRef(const TSourceRef& source) {
    switch (source.Wrapper) {
    case ESourceWrapper::None:
        return source.Name;
    case ESourceWrapper::Sum:
        return TString::Join("SUM(", source.Name, ")");
    case ESourceWrapper::Max:
        return TString::Join("MAX(", source.Name, ")");
    case ESourceWrapper::Hist:
        return TString::Join("HIST(", source.Name, ")");
    }

    Y_ABORT("unexpected source wrapper %d", static_cast<int>(source.Wrapper));
}

bool FinalizeDescriptor(TDetailedMetricsDescriptor& descriptor, TString* error) {
    THashSet<TString> allNames;

    FinalizeMetrics(EMetricKind::Gauge, descriptor.Gauges, allNames, descriptor.Errors);
    FinalizeMetrics(EMetricKind::Rate, descriptor.Rates, allNames, descriptor.Errors);
    FinalizeMetrics(EMetricKind::Histogram, descriptor.Histograms, allNames, descriptor.Errors);

    descriptor.RawNames = {};
    CollectRawNames(descriptor.Gauges, descriptor.RawNames);
    CollectRawNames(descriptor.Rates, descriptor.RawNames);
    CollectRawNames(descriptor.Histograms, descriptor.RawNames);

    if (error) {
        *error = JoinSeq("; ", descriptor.Errors);
    }

    return descriptor.Errors.empty();
}

const TDetailedMetricsDescriptor* GetDetailedMetricsDescriptor(TTabletTypes::EType tabletType) {
    switch (tabletType) {
    case TTabletTypes::DataShard:
        return GetDataShardDescriptor();

    default:
        return nullptr;
    }
}

} // namespace NKikimr
