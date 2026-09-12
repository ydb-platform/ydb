#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_counter_set.h"
#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/sys_view/service/db_counters_codec.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/core/tablet/tablet_counters_app.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace {

using TMetricsSettings = NKikimrSchemeOp::TTableDetailedMetricsSettings;
using TTabletKey = std::pair<ui64, ui32>;
using TBucketKey = TMaybe<TTabletKey>; // Empty identifies the TABLE partial.
using TContribution = std::pair<TString, TBucketKey>;
using TContributions = THashSet<TContribution>;
using TStreamKey = std::pair<ui32, bool>;

TString SourceId(const TBucketKey& key) {
    return key ? TStringBuilder() << key->first << ':' << key->second : TString("table");
}

// A TABLE partial and a PARTITION leaf have the same publication pipeline.
// Cumulative/HIST history survives removal of a node while the bucket remains live.
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
        bool singleOwner
    )
        : SingleOwner(singleOwner)
        , ExecutorCounters(rawGroup->GetSubgroup("type", TTabletTypes::TypeToStr(type))
            ->GetSubgroup("category", "executor"), rawGroup->Visibility())
        , AppCounters(rawGroup->GetSubgroup("type", TTabletTypes::TypeToStr(type))
            ->GetSubgroup("category", "app"), rawGroup->Visibility())
        , Mapper(CreateYdbMetricsMapperByTabletType(type, targetGroup, rawGroup))
    {
        ExecutorCounters.Initialize(executorTemplate, &names.ExecutorNames);
        auto appTemplate = CreateAppCountersByTabletType(type);
        AppCounters.Initialize(appTemplate.Get(), &names.AppNames);
        Total.SetType(type);
    }

    void Apply(ui32 nodeId, const NKikimrSysView::TDbTabletCounters& diff) {
        NSysView::TAggregateCumulative<false>::Apply(Total.MutableExecutorCounters(), diff.GetExecutorCounters());
        NSysView::TAggregateCumulative<false>::Apply(Total.MutableAppCounters(), diff.GetAppCounters());
        PerNode[nodeId] = diff;
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
        for (const auto& [_, snapshot] : PerNode) {
            AggregateSimple(Total.MutableExecutorCounters(), snapshot.GetExecutorCounters());
            AggregateSimple(Total.MutableAppCounters(), snapshot.GetAppCounters());
            AggregateMax(Total.MutableMaxExecutorCounters(), snapshot.GetMaxExecutorCounters());
            AggregateMax(Total.MutableMaxAppCounters(), snapshot.GetMaxAppCounters());
        }
        ExecutorCounters.FromProto(*Total.MutableExecutorCounters(), *Total.MutableMaxExecutorCounters());
        AppCounters.FromProto(*Total.MutableAppCounters(), *Total.MutableMaxAppCounters());
        Mapper->TransferCounterValues();
    }

private:
    void AggregateSimple(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) const {
        if (SingleOwner) {
            NSysView::TAggregateSimple<true>::Apply(dst, src);
        } else {
            NSysView::TAggregateSimple<false>::Apply(dst, src);
        }
    }

    static void AggregateMax(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        NSysView::TAggregateSimple<true>::Apply(dst, src);
        NSysView::TAggregateCumulative<true>::Apply(dst, src);
    }

    const bool SingleOwner;
    NPrivate::TAggregatedTabletCounters ExecutorCounters;
    NPrivate::TAggregatedTabletCounters AppCounters;
    TYdbMetricsMapperPtr Mapper;
    NKikimrSysView::TDbTabletCounters Total;
    THashMap<ui32, NKikimrSysView::TDbTabletCounters> PerNode;
};

struct TTableEntry {
    TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
    NMonitoring::TDynamicCounterPtr RawGroup;
    NMonitoring::TDynamicCounterPtr PublicGroup;
    TYdbMetricsAggregatorPtr Aggregator;
    THashMap<TBucketKey, THolder<TPublishedBucket>> Buckets;
};

class TProcessorDatabaseMetricsAggregatorImpl : public TProcessorDatabaseMetricsAggregator {
public:
    TProcessorDatabaseMetricsAggregatorImpl(
        NMonitoring::TDynamicCounterPtr rawCounterGroup,
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        THolder<TTabletCountersBase> executorCountersTemplate
    )
        : RawCounterGroup(rawCounterGroup)
        , TargetCounterGroup(targetCounterGroup)
        , DatabasePrefix(databasePath)
        , ExecutorCountersTemplate(std::move(executorCountersTemplate))
    {
        if (DatabasePrefix.EndsWith('/')) {
            DatabasePrefix.resize(DatabasePrefix.size() - 1);
        }
        Y_ABORT_UNLESS(ExecutorCountersTemplate, "executorCountersTemplate must not be null");
    }

    void ApplyFromNode(
        ui32 nodeId,
        bool isFollowerRole,
        const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables
    ) override {
        TContributions contributions;
        for (const auto& table : tables) {
            const TString path = RelativePath(table.GetTablePath());
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
        ReconcileStream({nodeId, isFollowerRole}, std::move(contributions));
    }

    void DropNode(ui32 nodeId) override {
        ReconcileStream({nodeId, false}, {});
        ReconcileStream({nodeId, true}, {});
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
    TString RelativePath(const TString& path) const {
        TStringBuf relative(path);
        if (relative.SkipPrefix(DatabasePrefix) && relative.SkipPrefix("/") && !relative.empty()) {
            return TString(relative);
        }
        return path;
    }

    void ApplyContribution(
        ui32 nodeId,
        const TContribution& contribution,
        const NKikimrSysView::TDbTabletCounters& diff,
        TContributions& contributions
    ) {
        const auto& [path, key] = contribution;
        const auto type = diff.GetType();
        const auto* names = GetDetailedMetricsCounterNames(type);
        if (path.empty() || !names) {
            return;
        }
        auto& table = Tables[path];
        if (table.Type == TTabletTypes::TypeInvalid) {
            table.Type = type;
            table.RawGroup = RawCounterGroup->GetSubgroup("table", path);
            table.PublicGroup = TargetCounterGroup->GetSubgroup("table", path);
            table.Aggregator = CreateYdbMetricsAggregatorByTabletType(type, table.PublicGroup);
        } else if (table.Type != type) {
            return;
        }
        auto& bucket = table.Buckets[key];
        if (!bucket) {
            auto rawGroup = table.RawGroup;
            auto mappedGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
            if (key) {
                rawGroup = rawGroup->GetSubgroup("detailed_metrics", "per_partition")
                    ->GetSubgroup("tablet_id", ToString(key->first))
                    ->GetSubgroup("follower_id", ToString(key->second));
                mappedGroup = table.PublicGroup->GetSubgroup("tablet_id", ToString(key->first))
                    ->GetSubgroup("follower_id", ToString(key->second));
            }
            // The partial's mapped group is detached: only the combined table
            // rollup is public, so partials and leaves never overwrite each other.
            bucket = MakeHolder<TPublishedBucket>(rawGroup, mappedGroup, type, *names,
                ExecutorCountersTemplate.Get(), key.Defined());
            table.Aggregator->AddSourceCountersGroup(SourceId(key), mappedGroup, key && key->second != 0);
        }
        bucket->Apply(nodeId, diff);
        contributions.insert(contribution);
    }

    void ReconcileStream(const TStreamKey& stream, TContributions contributions) {
        auto& previous = StreamContributions[stream];
        for (const auto& contribution : previous) {
            if (!contributions.contains(contribution)) {
                RemoveContribution(stream.first, contribution);
            }
        }
        if (contributions.empty()) {
            StreamContributions.erase(stream);
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
        table.Aggregator->RemoveSourceCountersGroup(SourceId(key));
        table.Buckets.erase(bucketIt);
        if (key) {
            table.RawGroup->RemoveSubgroupChain({
                {"detailed_metrics", "per_partition"},
                {"tablet_id", ToString(key->first)}, {"follower_id", ToString(key->second)},
            });
            table.PublicGroup->RemoveSubgroupChain({
                {"tablet_id", ToString(key->first)}, {"follower_id", ToString(key->second)},
            });
        } else {
            table.RawGroup->RemoveSubgroup("type", TTabletTypes::TypeToStr(table.Type));
        }
        if (table.Buckets.empty()) {
            RawCounterGroup->RemoveSubgroup("table", path);
            TargetCounterGroup->RemoveSubgroup("table", path);
            Tables.erase(tableIt);
        }
    }

    NMonitoring::TDynamicCounterPtr RawCounterGroup;
    NMonitoring::TDynamicCounterPtr TargetCounterGroup;
    TString DatabasePrefix;
    THolder<TTabletCountersBase> ExecutorCountersTemplate;
    THashMap<TString, TTableEntry> Tables;
    THashMap<TStreamKey, TContributions> StreamContributions;
};

} // namespace

TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr rawCounterGroup,
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    THolder<TTabletCountersBase> executorCountersTemplate
) {
    return MakeIntrusive<TProcessorDatabaseMetricsAggregatorImpl>(
        rawCounterGroup, targetCounterGroup, databasePath, std::move(executorCountersTemplate));
}

} // namespace NKikimr
