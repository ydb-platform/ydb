#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_binding.h"
#include "detailed_metrics_descriptor.h"
#include "detailed_metrics_tree.h"
#include "memory_tags.h"
#include "public_metrics_bucket.h"
#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/prof/tag.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <array>
#include <tuple>
#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
    namespace {

        using namespace NDetailedMetrics;

        using TMetricsSettings = NKikimrSchemeOp::TTableDetailedMetricsSettings;
        // Contributions use the relative path identifying their published table group.
        using TContribution = std::pair<TString, TBucketKey>;
        using TContributions = THashSet<TContribution>;
        using TNodeRoleKey = std::pair<ui32, bool>;

        TString SourceId(const TBucketKey& key) {
            return key ? TStringBuilder() << key->first << ':' << key->second : TString("table");
        }

        /**
         * Converts the low level counters of a bucket, which the nodes report, into the public
         * metric values of the bucket (see TPublicBucket for their encoding): the same values,
         * which the YDB metrics mapper used to publish from the same low level counters.
         *
         * The low level counters of a bucket are the full counter layout of its tablet type,
         * aggregated by the node over the tablets of the bucket:
         * - Simple of ExecutorCounters and AppCounters holds the sums over the tablets,
         *   Simple of MaxExecutorCounters and MaxAppCounters holds the maximums,
         *   both are the current values on the node;
         * - Cumulative holds sparse (slot, delta since the previous report) pairs;
         * - Histogram holds the HIST(x) and the integral percentile counters marked NonDerivative
         *   with their full current buckets, and the derivative ones with the bucket deltas.
         *
         * The descriptor of the tablet type is bound to the counter templates of the tablet type
         * (the Executor counters template and the application counters of the tablet type):
         * their slots are the slots of the reported layout, which only appends more counters
         * after them (for example, the DataShard transaction type counters).
         *
         * @note Temporary: only until the nodes report the public metric values themselves.
         */
        class TLegacyConverter {
        public:
            explicit TLegacyConverter(THolder<TTabletCountersBase> executorTemplate)
                : ExecutorTemplate(std::move(executorTemplate))
            {
                Y_ABORT_UNLESS(ExecutorTemplate, "executorCountersTemplate must not be null");
            }

            /**
             * Convert the low level counters of a bucket into its public metric values.
             *
             * @param[in] legacy The low level counters reported by a node
             * @param[out] out The public metric values (cleared first): dense Simple,
             *             sparse Cumulative, one Histogram entry per histogram metric
             *
             * @return False (and nothing is converted) if the tablet type has no detailed metrics
             */
            bool Convert(const NKikimrSysView::TDbTabletCounters& legacy, NKikimrSysView::TDbCounters& out) {
                const TBoundType* bound = GetOrBind(legacy.GetType());
                if (!bound) {
                    return false;
                }

                const auto& binding = *bound->Binding;
                const auto& descriptor = *binding.Descriptor;

                out.Clear();

                // Gauges: SUM(x) is the sum over the tablets of the bucket, MAX(x) is the maximum,
                // the metric value is the sum of its terms
                auto* simple = out.MutableSimple();
                simple->Resize(static_cast<int>(descriptor.Gauges.size()), 0);

                for (const auto& term : binding.Terms) {
                    if (term.Op == ESourceOp::SimpleSum) {
                        (*simple)[term.Metric] += GetSimple(GetCounters(legacy, term.Bank), term.Slot);
                    } else if (term.Op == ESourceOp::SimpleMax) {
                        (*simple)[term.Metric] += GetSimple(GetMaxCounters(legacy, term.Bank), term.Slot);
                    }
                }

                // Rates: the deltas of every term of the metric
                RateDeltas.assign(descriptor.Rates.size(), 0);

                for (const EBank bank : {EBank::Executor, EBank::App}) {
                    AddRateDeltas(bound->RateSources[static_cast<size_t>(bank)], GetCounters(legacy, bank));
                }

                out.SetCumulativeCount(descriptor.Rates.size());

                for (size_t metric = 0; metric < RateDeltas.size(); ++metric) {
                    if (RateDeltas[metric]) {
                        out.AddCumulative(metric);
                        out.AddCumulative(RateDeltas[metric]);
                    }
                }

                // Histograms: every histogram metric is present, even if it is empty
                for (ui32 metric = 0; metric < descriptor.Histograms.size(); ++metric) {
                    const auto& spec = descriptor.Histograms[metric];
                    HistogramBuckets.assign(spec.BucketCount(), 0);

                    for (const auto& term : binding.Terms) {
                        if (term.Kind == EMetricKind::Histogram && term.Metric == metric) {
                            AddHistogramBuckets(spec, GetCounters(legacy, term.Bank), GetHistogramSlot(term));
                        }
                    }

                    auto* histogram = out.AddHistogram();
                    histogram->SetBucketsCount(spec.BucketCount());
                    if (spec.IsLevel) {
                        histogram->SetNonDerivative(true);
                    }

                    for (size_t bucket = 0; bucket < HistogramBuckets.size(); ++bucket) {
                        if (HistogramBuckets[bucket]) {
                            histogram->AddBuckets(bucket);
                            histogram->AddBuckets(HistogramBuckets[bucket]);
                        }
                    }
                }

                return true;
            }

            /**
             * Take the warnings added since the previous call: the problems of every new binding
             * and the first histogram, whose NonDerivative mark disagrees with the descriptor.
             */
            TVector<TString> TakeWarnings() {
                return std::exchange(Warnings, {});
            }

        private:
            /**
             * A rate term: the slot of its cumulative counter and the rate metric.
             */
            struct TRateSource {
                ui32 Slot = 0;
                ui32 Metric = 0;

                bool operator<(const TRateSource& other) const {
                    return std::tie(Slot, Metric) < std::tie(other.Slot, other.Metric);
                }
            };

            struct TBoundType {
                THolder<TTabletCountersBase> AppTemplate;
                THolder<TDetailedMetricsBinding> Binding;

                /**
                 * The rate terms of the Executor and the application counters (the index is
                 * the bank), sorted by the slot.
                 */
                std::array<TVector<TRateSource>, 2> RateSources;
            };

            const TBoundType* GetOrBind(TTabletTypes::EType type) {
                if (auto it = BoundTypes.find(type); it != BoundTypes.end()) {
                    return &it->second;
                }

                const auto* descriptor = GetDetailedMetricsDescriptor(type);
                if (!descriptor) {
                    return nullptr;
                }

                auto& bound = BoundTypes[type];
                bound.AppTemplate = CreateAppCountersByTabletType(type);
                bound.Binding = BindDetailedMetrics(*descriptor, *ExecutorTemplate, *bound.AppTemplate);

                for (const auto& term : bound.Binding->Terms) {
                    if (term.Op == ESourceOp::CumulativeDelta) {
                        bound.RateSources[static_cast<size_t>(term.Bank)].push_back({term.Slot, term.Metric});
                    }
                }
                for (auto& sources : bound.RateSources) {
                    Sort(sources);
                }

                for (const auto& problem : bound.Binding->Problems) {
                    Warnings.push_back(TStringBuilder()
                        << "tablet type " << TTabletTypes::TypeToStr(type) << ": " << problem);
                }

                return &bound;
            }

            static const NKikimrSysView::TDbCounters& GetCounters(
                const NKikimrSysView::TDbTabletCounters& legacy, EBank bank)
            {
                return bank == EBank::Executor ? legacy.GetExecutorCounters() : legacy.GetAppCounters();
            }

            static const NKikimrSysView::TDbCounters& GetMaxCounters(
                const NKikimrSysView::TDbTabletCounters& legacy, EBank bank)
            {
                return bank == EBank::Executor ? legacy.GetMaxExecutorCounters() : legacy.GetMaxAppCounters();
            }

            static ui64 GetSimple(const NKikimrSysView::TDbCounters& counters, ui32 slot) {
                return slot < static_cast<ui32>(counters.SimpleSize()) ? counters.GetSimple(slot) : 0;
            }

            /**
             * @return The slot of the percentile counter, which holds the buckets of the term
             */
            static ui32 GetHistogramSlot(const TBoundTerm& term) {
                return term.Op == ESourceOp::HistOfSimple || term.Op == ESourceOp::HistOfCumulative
                    ? term.HistSlot
                    : term.Slot;
            }

            void AddRateDeltas(const TVector<TRateSource>& sources, const NKikimrSysView::TDbCounters& counters) {
                if (sources.empty()) {
                    return;
                }

                // The same pairs as NSysView::TAggregateCumulative applies: an odd tail
                // and a slot beyond CumulativeCount are ignored
                const ui64 cumulativeCount = counters.GetCumulativeCount();
                const auto& pairs = counters.GetCumulative();

                for (int i = 0; i + 1 < pairs.size(); i += 2) {
                    const ui64 slot = pairs[i];
                    if (slot >= cumulativeCount) {
                        continue;
                    }

                    auto it = LowerBoundBy(sources.begin(), sources.end(), slot,
                        [](const TRateSource& source) { return static_cast<ui64>(source.Slot); });

                    for (; it != sources.end() && it->Slot == slot; ++it) {
                        RateDeltas[it->Metric] += pairs[i + 1];
                    }
                }
            }

            void AddHistogramBuckets(const TMetricSpec& spec, const NKikimrSysView::TDbCounters& counters, ui32 slot) {
                // A missing histogram contributes nothing
                if (slot >= static_cast<ui32>(counters.HistogramSize())) {
                    return;
                }

                const auto& histogram = counters.GetHistogram(slot);

                if (histogram.GetNonDerivative() != spec.IsLevel) {
                    // An unmarked level is a delta, which cannot be applied without the baseline
                    // it was taken against, and marked increments cannot be added: either way
                    // the histogram contributes nothing to this report of the node
                    if (!WarnedMarkMismatch) {
                        WarnedMarkMismatch = true;
                        Warnings.push_back(TStringBuilder()
                            << "the histogram '" << spec.Name << "' holds " << (spec.IsLevel ? "a level" : "increments")
                            << ", but its source percentile counter #" << slot << " is "
                            << (spec.IsLevel ? "not " : "") << "marked NonDerivative, the source is ignored");
                    }
                    return;
                }

                // The extra buckets of the source (beyond its own bucket count or beyond
                // the public buckets) are ignored, the same way as the YDB metrics mapper
                // never saw them
                const ui64 bucketCount = Min<ui64>(histogram.GetBucketsCount(), HistogramBuckets.size());
                const auto& pairs = histogram.GetBuckets();

                for (int i = 0; i + 1 < pairs.size(); i += 2) {
                    if (pairs[i] < bucketCount) {
                        HistogramBuckets[pairs[i]] += pairs[i + 1];
                    }
                }
            }

            THolder<TTabletCountersBase> ExecutorTemplate;
            THashMap<TTabletTypes::EType, TBoundType> BoundTypes;

            // Scratch space of Convert(), kept to avoid allocations in the steady state
            TVector<ui64> RateDeltas;
            TVector<ui64> HistogramBuckets;

            TVector<TString> Warnings;
            bool WarnedMarkMismatch = false;
        };

        /**
         * Everything the processor keeps for a single table: the public metric values
         * of every bucket (TABLE partials and PARTITION leaves) and the table rollup.
         */
        struct TTableEntry {
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
            const TDetailedMetricsDescriptor* Desc = nullptr;
            NMonitoring::TDynamicCounterPtr PublicGroup;
            TYdbMetricsAggregatorPtr Aggregator;
            THashMap<TBucketKey, THolder<TPublicBucket>> Buckets;
        };

        class TProcessorDatabaseMetricsAggregatorImpl: public TProcessorDatabaseMetricsAggregator {
        public:
            TProcessorDatabaseMetricsAggregatorImpl(
                NMonitoring::TDynamicCounterPtr targetCounterGroup,
                const TString& databasePath,
                THolder<TTabletCountersBase> executorCountersTemplate)
                : TargetCounterGroup(targetCounterGroup)
                , DatabasePrefix(ChopTrailingSlash(databasePath))
                , Converter(std::move(executorCountersTemplate))
            {
            }

            void ApplyFromNode(
                ui32 nodeId,
                bool isFollowerRole,
                const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables) override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                TContributions contributions;
                for (const auto& table : tables) {
                    const TString path(MakeRelativeTablePath(DatabasePrefix, table.GetTablePath()));
                    if (table.GetLevel() == TMetricsSettings::MetricsLevelTable && !isFollowerRole) {
                        ApplyContribution(nodeId, {path, Nothing()}, table.GetTableCounters(), contributions);
                    } else if (table.GetLevel() == TMetricsSettings::MetricsLevelPartition) {
                        for (const auto& leaf : table.GetLeaves()) {
                            if ((leaf.GetFollowerId() != 0) == isFollowerRole) {
                                ApplyContribution(nodeId, {path, TTabletKey(leaf.GetTabletId(), leaf.GetFollowerId())},
                                                  leaf.GetCounters(), contributions);
                            }
                        }
                    }
                }
                // Register the new shape before retiring the old one, keeping the table
                // and its rollup alive throughout a gradual metrics-level change.
                ReconcileContributions({nodeId, isFollowerRole}, std::move(contributions));
            }

            void DropNode(ui32 nodeId) override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                ReconcileContributions({nodeId, false}, {});
                ReconcileContributions({nodeId, true}, {});
            }

            void RecalculateAllCounters() override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                for (auto& [_, table] : Tables) {
                    for (auto& [key, bucket] : table.Buckets) {
                        bucket->Publish();
                    }
                    table.Aggregator->RecalculateAllTargetCounters();
                }
            }

        private:
            void ApplyContribution(
                ui32 nodeId,
                const TContribution& contribution,
                const NKikimrSysView::TDbTabletCounters& diff,
                TContributions& contributions) {
                const auto& [path, key] = contribution;
                const auto type = diff.GetType();
                const auto* descriptor = GetDetailedMetricsDescriptor(type);
                if (path.empty() || !descriptor) {
                    return;
                }
                auto& table = Tables[path];
                if (table.Type == TTabletTypes::TypeInvalid) {
                    table.Type = type;
                    table.Desc = descriptor;
                    table.PublicGroup = TargetCounterGroup->GetSubgroup(TABLE_LABEL, path);
                    table.Aggregator = CreateYdbMetricsAggregatorByTabletType(
                        type, table.PublicGroup, ECumulativeHistoryPolicy::RetainOnSourceRemoval);
                } else if (table.Type != type) {
                    return;
                }
                auto& bucket = table.Buckets[key];
                if (!bucket) {
                    // The partial's group is detached: only the combined table rollup
                    // is public, so partials and leaves never overwrite each other.
                    const auto group = key
                        ? GetOrCreateTabletGroup(table.PublicGroup, *key)
                        : MakeIntrusive<NMonitoring::TDynamicCounters>();
                    const EYdbMetricNameScope nameScope = key
                        ? EYdbMetricNameScope::Partition
                        : EYdbMetricNameScope::Aggregate;
                    const bool isFollowerSource = key && key->second != 0;
                    // The bucket creates every target, which the rollup looks up below
                    bucket = MakeHolder<TPublicBucket>(*table.Desc, group, key.Defined(), isFollowerSource);
                    table.Aggregator->AddSourceCountersGroup(SourceId(key), group, isFollowerSource, nameScope);
                }
                if (Converter.Convert(diff, Converted)) {
                    bucket->Apply(nodeId, Converted);
                }
                LogWarnings(nodeId, path, Converter.TakeWarnings());
                LogWarnings(nodeId, path, bucket->TakeWarnings());
                contributions.insert(contribution);
            }

            void LogWarnings(ui32 nodeId, const TString& path, const TVector<TString>& warnings) const {
                for (const auto& warning : warnings) {
                    YDB_LOG_WARN("Problem with the detailed metrics reported by a node",
                        {"database", DatabasePrefix},
                        {"table", path},
                        {"nodeId", nodeId},
                        {"warning", warning});
                }
            }

            void ReconcileContributions(const TNodeRoleKey& nodeRole, TContributions contributions) {
                auto& previous = ContributionsByNodeRole[nodeRole];
                for (const auto& contribution : previous) {
                    if (!contributions.contains(contribution)) {
                        RemoveContribution(nodeRole.first, contribution);
                    }
                }
                if (contributions.empty()) {
                    ContributionsByNodeRole.erase(nodeRole);
                } else {
                    previous = std::move(contributions);
                }
            }

            void RemoveContribution(ui32 nodeId, const TContribution& contribution) {
                const auto& [path, key] = contribution;
                auto tableIt = Tables.find(path);
                if (tableIt == Tables.end()) {
                    return;
                }
                auto& table = tableIt->second;
                auto bucketIt = table.Buckets.find(key);
                if (bucketIt == table.Buckets.end() || !bucketIt->second->DropNode(nodeId)) {
                    return;
                }
                // The last report may not have been published yet. Retain its cumulative
                // values in the table rollup before detaching this source.
                bucketIt->second->Publish();
                table.Aggregator->RemoveSourceCountersGroup(SourceId(key));
                table.Buckets.erase(bucketIt);
                if (key) {
                    table.PublicGroup->RemoveSubgroupChain(MakeTabletPath(*key));
                }
                if (table.Buckets.empty()) {
                    TargetCounterGroup->RemoveSubgroup(TABLE_LABEL, path);
                    Tables.erase(tableIt);
                }
            }

            NMonitoring::TDynamicCounterPtr TargetCounterGroup;
            const TString DatabasePrefix;
            TLegacyConverter Converter;
            // The converted values of the bucket being applied, kept to reuse its memory
            NKikimrSysView::TDbCounters Converted;
            THashMap<TString, TTableEntry> Tables;
            THashMap<TNodeRoleKey, TContributions> ContributionsByNodeRole;
        };

    } // namespace

    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        THolder<TTabletCountersBase> executorCountersTemplate) {
        NProfiling::TMemoryTagScope memoryScope(NDetailedMetrics::ProcessorMemoryTag());
        return MakeIntrusive<TProcessorDatabaseMetricsAggregatorImpl>(
            targetCounterGroup, databasePath, std::move(executorCountersTemplate));
    }

} // namespace NKikimr
