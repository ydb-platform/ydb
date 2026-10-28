#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_tree.h"
#include "memory_tags.h"
#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

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

        // A TABLE partial or a PARTITION leaf, fed with public metric values (see NKikimrSysView::TDbCounters).
        // Rate and derivative histogram deltas accumulate and outlive a removed node while the bucket is live.
        // Gauges and non-derivative histograms are the full values of each node, replaced by every report
        // and combined on publish. The input is remote: out of range slots and buckets are ignored, the
        // payload sizes nothing.
        class TPublishedBucket {
        public:
            TPublishedBucket(
                const TDetailedMetricsDescriptor& desc,
                NMonitoring::TDynamicCounterPtr targetGroup,
                EYdbMetricNameScope nameScope,
                bool isFollowerSource)
                : Desc(desc)
                , IsPartitionBucket(nameScope == EYdbMetricNameScope::Partition)
                , RateTotals(desc.Rates.size(), 0)
                , DerivativeTotals(desc.Histograms.size())
            {
                // The same targets as the YDB metrics mapper creates: the rollup looks them up
                const auto getName = [&](const TMetricSpec& spec) {
                    return MakeYdbMetricName(spec.Name, nameScope);
                };
                const auto isSkipped = [&](const TMetricSpec& spec) {
                    return isFollowerSource && spec.LeaderOnly;
                };
                for (const auto& spec : desc.Gauges) {
                    auto& target = Gauges.emplace_back();
                    if (!isSkipped(spec)) {
                        target = targetGroup->GetNamedCounter("name", getName(spec), false /* derivative */);
                    }
                }
                for (const auto& spec : desc.Rates) {
                    auto& target = Rates.emplace_back();
                    if (!isSkipped(spec)) {
                        target = targetGroup->GetNamedCounter("name", getName(spec), true /* derivative */);
                    }
                }
                for (size_t i = 0; i < desc.Histograms.size(); ++i) {
                    const auto& spec = desc.Histograms[i];
                    auto& target = Histograms.emplace_back();
                    if (!isSkipped(spec)) {
                        target = targetGroup->GetNamedHistogram("name", getName(spec),
                            NMonitoring::ExplicitHistogram(NMonitoring::TBucketBounds(spec.Bounds.begin(), spec.Bounds.end())),
                            false /* derivative */);
                    }
                    if (!spec.NonDerivative) {
                        DerivativeTotals[i].resize(spec.BucketCount(), 0);
                    }
                }
            }

            void Apply(ui32 nodeId, const NKikimrSysView::TDbCounters& values) {
                auto& node = PerNode[nodeId];
                node.Gauges.assign(Desc.Gauges.size(), 0);
                for (size_t i = 0; i < node.Gauges.size() && i < static_cast<size_t>(values.SimpleSize()); ++i) {
                    node.Gauges[i] = values.GetSimple(i);
                }
                const auto& cumulative = values.GetCumulative();
                for (int pair = 0; pair + 1 < cumulative.size(); pair += 2) {
                    if (cumulative[pair] < RateTotals.size()) {
                        RateTotals[cumulative[pair]] += cumulative[pair + 1];
                    }
                }
                node.NonDerivativeHists.resize(Desc.Histograms.size());
                for (size_t i = 0; i < Desc.Histograms.size(); ++i) {
                    ApplyHistogram(nodeId, i, i < static_cast<size_t>(values.HistogramSize()) ? &values.GetHistogram(i) : nullptr, node.NonDerivativeHists[i]);
                }
            }

            bool DropNode(ui32 nodeId) {
                PerNode.erase(nodeId);
                return PerNode.empty();
            }

            void Publish() {
                for (size_t i = 0; i < Gauges.size(); ++i) {
                    if (!Gauges[i]) {
                        continue;
                    }
                    // The partials of a table come from different tablets, so they add up unless CombineByMax;
                    // overlapping owners of a leaf use MAX, retaining the partition-move behavior
                    const bool combineByMax = IsPartitionBucket || Desc.Gauges[i].CombineByMax();
                    ui64 value = 0;
                    for (const auto& [_, node] : PerNode) {
                        value = combineByMax ? Max(value, node.Gauges[i]) : value + node.Gauges[i];
                    }
                    Gauges[i]->Set(value);
                }
                for (size_t i = 0; i < Rates.size(); ++i) {
                    if (Rates[i]) {
                        Rates[i]->Set(RateTotals[i]);
                    }
                }
                for (size_t i = 0; i < Histograms.size(); ++i) {
                    if (!Histograms[i]) {
                        continue;
                    }
                    const auto& spec = Desc.Histograms[i];
                    const bool nonDerivative = spec.NonDerivative;
                    Histograms[i]->Reset();
                    for (size_t bucket = 0; bucket < spec.BucketCount(); ++bucket) {
                        ui64 count = nonDerivative ? 0 : DerivativeTotals[i][bucket];
                        if (nonDerivative) {
                            for (const auto& [_, node] : PerNode) {
                                count += node.NonDerivativeHists[i][bucket];
                            }
                        }
                        if (count) {
                            // The upper bound of the bucket, as the YDB metrics mapper collects
                            Histograms[i]->Collect(bucket < spec.Bounds.size() ? spec.Bounds[bucket] : Max<double>(), count);
                        }
                    }
                }
            }

        private:
            struct TNodeSnapshot {
                TVector<ui64> Gauges;
                // Per histogram, empty for a derivative one
                TVector<TVector<ui64>> NonDerivativeHists;
            };

            void ApplyHistogram(
                ui32 nodeId, size_t index, const NKikimrSysView::TDbCounters::THistogram* histogram, TVector<ui64>& nodeHist)
            {
                const auto& spec = Desc.Histograms[index];
                const bool nonDerivative = spec.NonDerivative;
                if (nonDerivative) {
                    nodeHist.assign(spec.BucketCount(), 0);
                }
                if (!histogram) {
                    return;
                }
                if (histogram->GetNonDerivative() != nonDerivative) {
                    // A delta cannot be applied without the baseline it was taken against
                    if (!WarnedAboutNonDerivativeMismatch) {
                        WarnedAboutNonDerivativeMismatch = true;
                        YDB_LOG_WARN("Ignored a histogram, whose NonDerivative mark does not match the metric",
                            {"nodeId", nodeId},
                            {"histogram", spec.Name});
                    }
                    return;
                }
                auto& values = nonDerivative ? nodeHist : DerivativeTotals[index];
                const ui64 bucketCount = Min<ui64>(histogram->GetBucketsCount(), values.size());
                const auto& encoded = histogram->GetBuckets();
                for (int b = 0; b + 1 < encoded.size(); b += 2) {
                    if (encoded[b] >= bucketCount) {
                        continue;
                    }
                    if (nonDerivative) {
                        values[encoded[b]] = encoded[b + 1];
                    } else {
                        values[encoded[b]] += encoded[b + 1];
                    }
                }
            }

            const TDetailedMetricsDescriptor& Desc;
            const bool IsPartitionBucket;
            TVector<NMonitoring::TDynamicCounters::TCounterPtr> Gauges;
            TVector<NMonitoring::TDynamicCounters::TCounterPtr> Rates;
            TVector<NMonitoring::THistogramPtr> Histograms;
            TVector<ui64> RateTotals;
            // Per histogram, empty for a non-derivative one
            TVector<TVector<ui64>> DerivativeTotals;
            THashMap<ui32, TNodeSnapshot> PerNode;
            bool WarnedAboutNonDerivativeMismatch = false;
        };

        struct TTableEntry {
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
            NMonitoring::TDynamicCounterPtr PublicGroup;
            TYdbMetricsAggregatorPtr Aggregator;
            THashMap<TBucketKey, THolder<TPublishedBucket>> Buckets;
        };

        class TProcessorDatabaseMetricsAggregatorImpl: public TProcessorDatabaseMetricsAggregator {
        public:
            TProcessorDatabaseMetricsAggregatorImpl(
                NMonitoring::TDynamicCounterPtr targetCounterGroup,
                const TString& databasePath,
                TDetailedMetricsDescriptorGetter getDescriptor)
                : TargetCounterGroup(targetCounterGroup)
                , DatabasePrefix(ChopTrailingSlash(databasePath))
                , GetDescriptor(getDescriptor)
            {
            }

            void ApplyFromNode(
                ui32 nodeId,
                bool isFollowerRole,
                const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables) override {
                NProfiling::TMemoryTagScope memoryScope(ProcessorMemoryTag());
                TContributions contributions;
                for (const auto& table : tables) {
                    if (!table.HasTabletType()) {
                        continue;
                    }
                    const TString path(MakeRelativeTablePath(DatabasePrefix, table.GetTablePath()));
                    const auto type = table.GetTabletType();
                    if (table.GetLevel() == TMetricsSettings::MetricsLevelTable && !isFollowerRole) {
                        ApplyContribution(nodeId, {path, Nothing()}, type, table.GetTableMetrics(), contributions);
                    } else if (table.GetLevel() == TMetricsSettings::MetricsLevelPartition) {
                        for (const auto& leaf : table.GetLeaves()) {
                            if ((leaf.GetFollowerId() != 0) == isFollowerRole) {
                                ApplyContribution(nodeId, {path, TTabletKey(leaf.GetTabletId(), leaf.GetFollowerId())},
                                                  type, leaf.GetMetrics(), contributions);
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
                TTabletTypes::EType type,
                const NKikimrSysView::TDbCounters& values,
                TContributions& contributions) {
                const auto& [path, key] = contribution;
                const auto* descriptor = GetDescriptor(type);
                if (path.empty() || !descriptor) {
                    return;
                }
                auto& table = Tables[path];
                if (table.Type == TTabletTypes::TypeInvalid) {
                    table.Type = type;
                    table.PublicGroup = TargetCounterGroup->GetSubgroup(TABLE_LABEL, path);
                    table.Aggregator = CreateYdbMetricsAggregatorByTabletType(
                        type, table.PublicGroup, ECumulativeHistoryPolicy::RetainOnSourceRemoval);
                } else if (table.Type != type) {
                    return;
                }
                auto& bucket = table.Buckets[key];
                if (!bucket) {
                    auto mappedGroup = key
                        ? GetOrCreateTabletGroup(table.PublicGroup, *key)
                        : MakeIntrusive<NMonitoring::TDynamicCounters>();
                    // The partial's mapped group is detached: only the combined table
                    // rollup is public, so partials and leaves never overwrite each other.
                    const EYdbMetricNameScope nameScope = key
                        ? EYdbMetricNameScope::Partition
                        : EYdbMetricNameScope::Aggregate;
                    const bool isFollowerSource = key && key->second != 0;
                    bucket = MakeHolder<TPublishedBucket>(*descriptor, mappedGroup, nameScope, isFollowerSource);
                    table.Aggregator->AddSourceCountersGroup(SourceId(key), mappedGroup, isFollowerSource, nameScope);
                }
                bucket->Apply(nodeId, values);
                contributions.insert(contribution);
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
            const TDetailedMetricsDescriptorGetter GetDescriptor;
            THashMap<TString, TTableEntry> Tables;
            THashMap<TNodeRoleKey, TContributions> ContributionsByNodeRole;
        };

    } // namespace

    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        TDetailedMetricsDescriptorGetter getDescriptor) {
        NProfiling::TMemoryTagScope memoryScope(NDetailedMetrics::ProcessorMemoryTag());
        return MakeIntrusive<TProcessorDatabaseMetricsAggregatorImpl>(targetCounterGroup, databasePath, getDescriptor);
    }

} // namespace NKikimr
