#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_counter_set.h"
#include "detailed_metrics_tree.h"
#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/sys_view/service/db_counters_codec.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_app.h>
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

        // A TABLE partial and a PARTITION leaf have the same publication pipeline.
        // Cumulative and derivative histogram history survives removal of a node while
        // the bucket remains live. Live histogram totals are recomputed from the live
        // per-node snapshots on each publish
        // Simple/MAX are recomputed from live snapshots; overlapping owners of a leaf
        // use MAX for Simple, retaining the existing partition-move behavior.
        class TPublishedBucket {
        public:
            TPublishedBucket(
                NMonitoring::TDynamicCounterPtr rawGroup,
                NMonitoring::TDynamicCounterPtr targetGroup,
                TTabletTypes::EType type,
                const TDetailedMetricsCounterNames& names,
                const TTabletCountersBase* executorTemplate,
                const TTabletCountersBase* appTemplate,
                bool isPartitionBucket)
                : IsPartitionBucket(isPartitionBucket)
                , ExecutorCounters(GetOrCreateTypeGroup(rawGroup, type)
                                       ->GetSubgroup(CATEGORY_LABEL, EXECUTOR_CATEGORY))
                , AppCounters(GetOrCreateTypeGroup(rawGroup, type)
                                  ->GetSubgroup(CATEGORY_LABEL, APP_CATEGORY))
                , Mapper(CreateYdbMetricsMapperByTabletType(type, targetGroup, rawGroup))
            {
                ExecutorCounters.Initialize(executorTemplate, &names.ExecutorNames);
                AppCounters.Initialize(appTemplate, &names.AppNames);
                ExecutorLiveHistogramIndices = FindLiveHistogramIndices(*executorTemplate, names.ExecutorNames);
                AppLiveHistogramIndices = FindLiveHistogramIndices(*appTemplate, names.AppNames);
                Total.SetType(type);
            }

            void Apply(ui32 nodeId, const NKikimrSysView::TDbTabletCounters& diff) {
                NSysView::TAggregateCumulative<false>::Apply(Total.MutableExecutorCounters(), diff.GetExecutorCounters());
                NSysView::TAggregateCumulative<false>::Apply(Total.MutableAppCounters(), diff.GetAppCounters());
                auto& snapshot = PerNode[nodeId];
                snapshot.Counters = diff;
                ApplyLiveHistogramDeltas(snapshot.ExecutorHistogramBucketCounts, ExecutorLiveHistogramIndices,
                                         diff.GetExecutorCounters(), nodeId);
                ApplyLiveHistogramDeltas(snapshot.AppHistogramBucketCounts, AppLiveHistogramIndices,
                                         diff.GetAppCounters(), nodeId);
            }

            bool DropNode(ui32 nodeId) {
                PerNode.erase(nodeId);
                return PerNode.empty();
            }

            void Publish() {
                NSysView::ResetSimpleCounters(Total.MutableExecutorCounters());
                NSysView::ResetSimpleCounters(Total.MutableAppCounters());
                NSysView::ResetMaxCounters(Total.MutableMaxExecutorCounters());
                NSysView::ResetMaxCounters(Total.MutableMaxAppCounters());
                NSysView::ResetHistogramBuckets(Total.MutableExecutorCounters(), ExecutorLiveHistogramIndices);
                NSysView::ResetHistogramBuckets(Total.MutableAppCounters(), AppLiveHistogramIndices);
                for (const auto& [_, node] : PerNode) {
                    const auto& snapshot = node.Counters;
                    AggregateSimple(Total.MutableExecutorCounters(), snapshot.GetExecutorCounters());
                    AggregateSimple(Total.MutableAppCounters(), snapshot.GetAppCounters());
                    AggregateMax(Total.MutableMaxExecutorCounters(), snapshot.GetMaxExecutorCounters());
                    AggregateMax(Total.MutableMaxAppCounters(), snapshot.GetMaxAppCounters());
                    AddLiveHistogramBucketCounts(*Total.MutableExecutorCounters(), ExecutorLiveHistogramIndices,
                                                 node.ExecutorHistogramBucketCounts);
                    AddLiveHistogramBucketCounts(*Total.MutableAppCounters(), AppLiveHistogramIndices,
                                                 node.AppHistogramBucketCounts);
                }
                ExecutorCounters.FromProto(*Total.MutableExecutorCounters(), *Total.MutableMaxExecutorCounters());
                AppCounters.FromProto(*Total.MutableAppCounters(), *Total.MutableMaxAppCounters());
                Mapper->TransferCounterValues();
            }

        private:
            // Decoded per-node bucket counts, retained to subtract its contribution on removal.
            // The outer index follows the corresponding live histogram indices; the inner
            // index identifies a bucket within that histogram.
            using TLiveHistogramBucketCounts = TVector<TVector<ui64>>;

            struct TNodeSnapshot {
                NKikimrSysView::TDbTabletCounters Counters;
                TLiveHistogramBucketCounts ExecutorHistogramBucketCounts;
                TLiveHistogramBucketCounts AppHistogramBucketCounts;
            };

            static TVector<ui32> FindLiveHistogramIndices(
                const TTabletCountersBase& counters, const THashSet<TString>& names)
            {
                TVector<ui32> indices;
                for (ui32 i = 0; i < counters.Percentile().Size(); ++i) {
                    const char* name = counters.PercentileCounterName(i);
                    if (name && (names.empty() || names.contains(name)) && (counters.Percentile()[i].GetIntegral() || !GetHistogramAggregateSimpleName(name).empty()))
                    {
                        indices.push_back(i);
                    }
                }
                return indices;
            }

            static void ApplyLiveHistogramDeltas(
                TLiveHistogramBucketCounts& bucketCounts, const TVector<ui32>& indices, const NKikimrSysView::TDbCounters& diff,
                ui32 nodeId)
            {
                bucketCounts.resize(indices.size());
                for (size_t i = 0; i < indices.size(); ++i) {
                    if (indices[i] >= diff.HistogramSize()) {
                        continue;
                    }
                    const auto& histogram = diff.GetHistogram(indices[i]);
                    auto& values = bucketCounts[i];
                    if (values.size() < histogram.GetBucketsCount()) {
                        values.resize(histogram.GetBucketsCount(), 0);
                    }
                    const auto& encoded = histogram.GetBuckets();
                    for (int b = 0; b + 1 < encoded.size(); b += 2) {
                        if (encoded[b] < histogram.GetBucketsCount()) {
                            const ui64 delta = encoded[b + 1];
                            // Histogram decreases are encoded modulo 2^64; treat large values
                            // as decrements
                            const bool isDecrement = delta > (Max<ui64>() >> 1);
                            if (isDecrement) {
                                const ui64 magnitude = 0 - delta;
                                if (magnitude > values[encoded[b]]) {
                                    YDB_LOG_WARN("Clamped live histogram bucket to 0 to avoid underflow",
                                        {"nodeId", nodeId},
                                        {"histogramIndex", indices[i]},
                                        {"bucketIndex", encoded[b]},
                                        {"magnitude", magnitude});
                                    values[encoded[b]] = 0;
                                } else {
                                    values[encoded[b]] -= magnitude;
                                }
                            } else {
                                values[encoded[b]] += delta;
                            }
                        }
                    }
                }
            }

            static void AddLiveHistogramBucketCounts(
                NKikimrSysView::TDbCounters& total, const TVector<ui32>& indices, const TLiveHistogramBucketCounts& bucketCounts)
            {
                for (size_t i = 0; i < bucketCounts.size(); ++i) {
                    if (bucketCounts[i].empty()) {
                        continue;
                    }
                    if (indices[i] >= total.HistogramSize()) {
                        continue;
                    }
                    auto* values = total.MutableHistogram(indices[i])->MutableBuckets();
                    // FromProto trims histograms to the receiver's template. Ignore any
                    // extra sender buckets that are no longer present in the total.
                    for (size_t b = 0; b < bucketCounts[i].size() && b < static_cast<size_t>(values->size()); ++b) {
                        (*values)[b] += bucketCounts[i][b];
                    }
                }
            }

            void AggregateSimple(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) const {
                if (IsPartitionBucket) {
                    NSysView::TAggregateSimple<true>::Apply(dst, src);
                } else {
                    NSysView::TAggregateSimple<false>::Apply(dst, src);
                }
            }

            static void AggregateMax(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
                NSysView::TAggregateSimple<true>::Apply(dst, src);
                NSysView::TAggregateCumulative<true>::Apply(dst, src);
            }

            const bool IsPartitionBucket;
            NPrivate::TAggregatedTabletCounters ExecutorCounters;
            NPrivate::TAggregatedTabletCounters AppCounters;
            TYdbMetricsMapperPtr Mapper;
            NKikimrSysView::TDbTabletCounters Total;
            TVector<ui32> ExecutorLiveHistogramIndices;
            TVector<ui32> AppLiveHistogramIndices;
            THashMap<ui32, TNodeSnapshot> PerNode;
        };

        struct TTableEntry {
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
            NMonitoring::TDynamicCounterPtr RawGroup;
            NMonitoring::TDynamicCounterPtr PublicGroup;
            TYdbMetricsAggregatorPtr Aggregator;
            THashMap<TBucketKey, THolder<TPublishedBucket>> Buckets;
        };

        class TProcessorDatabaseMetricsAggregatorImpl: public TProcessorDatabaseMetricsAggregator {
        public:
            TProcessorDatabaseMetricsAggregatorImpl(
                NMonitoring::TDynamicCounterPtr rawCounterGroup,
                NMonitoring::TDynamicCounterPtr targetCounterGroup,
                const TString& databasePath,
                THolder<TTabletCountersBase> executorCountersTemplate)
                : RawCounterGroup(rawCounterGroup)
                , TargetCounterGroup(targetCounterGroup)
                , DatabasePrefix(ChopTrailingSlash(databasePath))
                , ExecutorCountersTemplate(std::move(executorCountersTemplate))
            {
                Y_ABORT_UNLESS(ExecutorCountersTemplate, "executorCountersTemplate must not be null");
            }

            void ApplyFromNode(
                ui32 nodeId,
                bool isFollowerRole,
                const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables) override {
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
                ReconcileContributions({nodeId, false}, {});
                ReconcileContributions({nodeId, true}, {});
            }

            void RecalculateAllCounters() override {
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
                const auto* names = GetDetailedMetricsCounterNames(type);
                if (path.empty() || !names) {
                    return;
                }
                auto& table = Tables[path];
                if (table.Type == TTabletTypes::TypeInvalid) {
                    table.Type = type;
                    table.RawGroup = RawCounterGroup->GetSubgroup(TABLE_LABEL, path);
                    table.PublicGroup = TargetCounterGroup->GetSubgroup(TABLE_LABEL, path);
                    table.Aggregator = CreateYdbMetricsAggregatorByTabletType(
                        type, table.PublicGroup, ECumulativeHistoryPolicy::RetainOnSourceRemoval);
                } else if (table.Type != type) {
                    return;
                }
                auto& bucket = table.Buckets[key];
                if (!bucket) {
                    auto rawGroup = table.RawGroup;
                    auto mappedGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
                    if (key) {
                        rawGroup = GetOrCreateTabletGroup(GetOrCreatePerPartitionGroup(rawGroup), *key);
                        mappedGroup = GetOrCreateTabletGroup(table.PublicGroup, *key);
                    }
                    // The partial's mapped group is detached: only the combined table
                    // rollup is public, so partials and leaves never overwrite each other.
                    auto appTemplate = CreateAppCountersByTabletType(type);
                    bucket = MakeHolder<TPublishedBucket>(rawGroup, mappedGroup, type, *names,
                                                          ExecutorCountersTemplate.Get(), appTemplate.Get(), key.Defined());
                    table.Aggregator->AddSourceCountersGroup(SourceId(key), mappedGroup, key && key->second != 0);
                }
                bucket->Apply(nodeId, diff);
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
                table.RawGroup->RemoveSubgroupChain(MakeRawBucketPath(key, table.Type));
                if (key) {
                    table.PublicGroup->RemoveSubgroupChain(MakeTabletPath(*key));
                }
                if (table.Buckets.empty()) {
                    RawCounterGroup->RemoveSubgroup(TABLE_LABEL, path);
                    TargetCounterGroup->RemoveSubgroup(TABLE_LABEL, path);
                    Tables.erase(tableIt);
                }
            }

            NMonitoring::TDynamicCounterPtr RawCounterGroup;
            NMonitoring::TDynamicCounterPtr TargetCounterGroup;
            const TString DatabasePrefix;
            THolder<TTabletCountersBase> ExecutorCountersTemplate;
            THashMap<TString, TTableEntry> Tables;
            THashMap<TNodeRoleKey, TContributions> ContributionsByNodeRole;
        };

    } // namespace

    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr rawCounterGroup,
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        THolder<TTabletCountersBase> executorCountersTemplate) {
        return MakeIntrusive<TProcessorDatabaseMetricsAggregatorImpl>(
            rawCounterGroup, targetCounterGroup, databasePath, std::move(executorCountersTemplate));
    }

} // namespace NKikimr
