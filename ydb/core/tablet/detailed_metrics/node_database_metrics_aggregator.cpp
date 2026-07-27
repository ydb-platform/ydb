#include "node_database_metrics_aggregator.h"

#include <ydb/core/tablet/private/aggregated_tablet_counters.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

namespace NKikimr {

namespace {

// Labels of the detailed metrics counter tree
const TString DATABASE_LABEL = "database";
const TString MONITORING_PROJECT_ID_LABEL = "monitoring_project_id";
const TString TABLE_LABEL = "table";
const TString ROLE_LABEL = "role";
const TString DETAILED_METRICS_LABEL = "detailed_metrics";
const TString TABLET_ID_LABEL = "tablet_id";
const TString FOLLOWER_ID_LABEL = "follower_id";

const TString LEADER_ROLE = "leader";
const TString FOLLOWER_ROLE = "follower";

const TString PER_PARTITION_VALUE = "per_partition";

// Labels of the low level tablet counters (the same as in the "tablets" group)
const TString TYPE_LABEL = "type";
const TString CATEGORY_LABEL = "category";

const TString EXECUTOR_CATEGORY = "executor";
const TString APP_CATEGORY = "app";

/**
 * A single tablet (a leader or a follower) within a table.
 */
using TTabletKey = std::pair<ui64, ui32>;

const TString& GetRoleLabelValue(ui32 followerId) {
    return followerId == 0 ? LEADER_ROLE : FOLLOWER_ROLE;
}

/**
 * Strip the database path prefix from the full path of the table.
 *
 * @note A path, which is not prefixed by the database path, is returned as is.
 *       That should never happen, but an odd looking "table" label is much easier
 *       to notice than silently dropped counters.
 */
TString MakeRelativeTablePath(const TString& databasePath, const TString& tablePath) {
    TStringBuf relativePath(tablePath);

    if (relativePath.SkipPrefix(databasePath)) {
        relativePath.SkipPrefix("/");
    }

    return TString(relativePath);
}

/**
 * A single bucket of the detailed metrics counter tree: the low level counters
 * of one or more tablets of the same type.
 *
 * @note A role bucket of a table level table holds many tablets, while a leaf group
 *       of a partition level table holds exactly one. Both cases are handled by
 *       the very same code: aggregating a single tablet is a passthrough
 *       (SUM(x) == MAX(x) == x).
 */
class TCountersBucket {
public:
    TCountersBucket(NMonitoring::TDynamicCounterPtr bucketGroup, TTabletTypes::EType tabletType)
        : TabletType(tabletType)
        , TypeGroup(bucketGroup->GetSubgroup(TYPE_LABEL, TString(TTabletTypes::TypeToStr(tabletType))))
        , ExecutorCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, EXECUTOR_CATEGORY))
        , AppCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, APP_CATEGORY))
    {}

    void Apply(
        const TTabletKey& tablet,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now
    ) {
        // The aggregates identify their sources by a single ui64, while a bucket may hold
        // several followers of the same tablet, hence the synthetic source IDs
        auto [it, inserted] = SourceIds.try_emplace(tablet, NextSourceId);
        if (inserted) {
            ++NextSourceId;
        }

        // The counter set layout is a property of the tablet type, so the very first
        // reporting tablet defines it once and for all
        if (!ExecutorCounters.IsInitialized) {
            ExecutorCounters.Initialize(&executorCounters);
        }
        if (!AppCounters.IsInitialized) {
            AppCounters.Initialize(&appCounters);
        }

        ExecutorCounters.Apply(it->second, &executorCounters, TabletType, now);
        AppCounters.Apply(it->second, &appCounters, TabletType, now);
    }

    void Forget(const TTabletKey& tablet) {
        auto it = SourceIds.find(tablet);
        if (it == SourceIds.end()) {
            return;
        }

        if (ExecutorCounters.IsInitialized) {
            ExecutorCounters.Forget(it->second);
        }
        if (AppCounters.IsInitialized) {
            AppCounters.Forget(it->second);
        }

        SourceIds.erase(it);
    }

    bool IsEmpty() const {
        return SourceIds.empty();
    }

    void RecalcAll() {
        if (ExecutorCounters.IsInitialized) {
            ExecutorCounters.RecalcAll();
        }
        if (AppCounters.IsInitialized) {
            AppCounters.RecalcAll();
        }
    }

private:
    TTabletTypes::EType TabletType;

    NMonitoring::TDynamicCounterPtr TypeGroup;

    NPrivate::TAggregatedTabletCounters ExecutorCounters;
    NPrivate::TAggregatedTabletCounters AppCounters;

    THashMap<TTabletKey, ui64> SourceIds;
    ui64 NextSourceId = 0;
};

/**
 * Everything the aggregator keeps for a single table.
 *
 * @note Only one of the two shapes is ever populated, the one chosen by
 *       the effective metrics level of the table.
 */
struct TTableEntry {
    TDetailedMetricsTableInfo Info;

    /**
     * The path of the table relative to the database (the "table" label).
     */
    TString RelativePath;

    NMonitoring::TDynamicCounterPtr TableGroup;

    // Table level, created on demand
    THolder<TCountersBucket> LeaderBucket;
    THolder<TCountersBucket> FollowerBucket;

    // Partition level, created on demand
    NMonitoring::TDynamicCounterPtr PerPartitionGroup;
    THashMap<TTabletKey, THolder<TCountersBucket>> Leaves;

    bool IsEmpty() const {
        return !LeaderBucket && !FollowerBucket && Leaves.empty();
    }

    THolder<TCountersBucket>& GetRoleBucket(ui32 followerId) {
        return followerId == 0 ? LeaderBucket : FollowerBucket;
    }
};

class TNodeDatabaseMetricsAggregatorImpl : public TNodeDatabaseMetricsAggregator {
public:
    TNodeDatabaseMetricsAggregatorImpl(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        const TString& monitoringProjectId
    )
        : TargetCounterGroup(targetCounterGroup)
        , DatabasePath(databasePath)
        , MonitoringProjectId(monitoringProjectId)
    {}

    void AddCounters(
        const TDetailedMetricsTableInfo& table,
        ui64 tabletId,
        ui32 followerId,
        TTabletTypes::EType tabletType,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now
    ) override {
        auto* entry = GetOrCreateTable(table);
        if (!entry) {
            return;
        }

        const TTabletKey tablet(tabletId, followerId);

        if (entry->Info.MetricsLevel == EDetailedMetricsLevel::Table) {
            auto& bucket = entry->GetRoleBucket(followerId);
            if (!bucket) {
                bucket = MakeHolder<TCountersBucket>(
                    entry->TableGroup->GetSubgroup(ROLE_LABEL, GetRoleLabelValue(followerId)),
                    tabletType
                );
            }
            bucket->Apply(tablet, executorCounters, appCounters, now);
        } else {
            auto& leaf = entry->Leaves[tablet];
            if (!leaf) {
                leaf = MakeHolder<TCountersBucket>(
                    GetOrCreatePerPartitionGroup(*entry)
                        ->GetSubgroup(TABLET_ID_LABEL, ToString(tabletId))
                        ->GetSubgroup(FOLLOWER_ID_LABEL, ToString(followerId)),
                    tabletType
                );
            }
            leaf->Apply(tablet, executorCounters, appCounters, now);
        }
    }

    void ForgetTablet(const TPathId& tableId, ui64 tabletId, ui32 followerId) override {
        auto it = Tables.find(tableId);
        if (it == Tables.end()) {
            return;
        }

        auto& entry = it->second;
        const TTabletKey tablet(tabletId, followerId);

        if (entry.Info.MetricsLevel == EDetailedMetricsLevel::Table) {
            ForgetRoleBucketTablet(entry, tablet);
        } else {
            ForgetLeaf(entry, tablet);
        }

        if (entry.IsEmpty()) {
            DatabaseGroup->RemoveSubgroup(TABLE_LABEL, entry.RelativePath);
            Tables.erase(it);
        }
    }

    void RecalculateAllCounters() override {
        for (auto& [_, entry] : Tables) {
            if (entry.LeaderBucket) {
                entry.LeaderBucket->RecalcAll();
            }
            if (entry.FollowerBucket) {
                entry.FollowerBucket->RecalcAll();
            }
            for (auto& [_, leaf] : entry.Leaves) {
                leaf->RecalcAll();
            }
        }
    }

private:
    NMonitoring::TDynamicCounterPtr GetOrCreateDatabaseGroup() {
        if (!DatabaseGroup) {
            DatabaseGroup = TargetCounterGroup->GetSubgroup(DATABASE_LABEL, DatabasePath);

            if (MonitoringProjectId) {
                DatabaseGroup = DatabaseGroup->GetSubgroup(
                    MONITORING_PROJECT_ID_LABEL,
                    MonitoringProjectId
                );
            }
        }

        return DatabaseGroup;
    }

    NMonitoring::TDynamicCounterPtr GetOrCreatePerPartitionGroup(TTableEntry& entry) {
        if (!entry.PerPartitionGroup) {
            entry.PerPartitionGroup = entry.TableGroup->GetSubgroup(
                DETAILED_METRICS_LABEL,
                PER_PARTITION_VALUE
            );
        }

        return entry.PerPartitionGroup;
    }

    /**
     * @return The per-table state, or nullptr if the table collects no detailed metrics
     */
    TTableEntry* GetOrCreateTable(const TDetailedMetricsTableInfo& table) {
        if (table.MetricsLevel != EDetailedMetricsLevel::Table &&
            table.MetricsLevel != EDetailedMetricsLevel::Partition)
        {
            return nullptr;
        }

        if (!table.TableId || !table.TablePath) {
            return nullptr;
        }

        auto it = Tables.find(table.TableId);
        if (it != Tables.end()) {
            return &it->second;
        }

        // NOTE: Reconciling an existing entry on a schema version or a metrics level
        //       change is implemented in a separate step

        auto& entry = Tables[table.TableId];
        entry.Info = table;
        entry.RelativePath = MakeRelativeTablePath(DatabasePath, table.TablePath);
        entry.TableGroup = GetOrCreateDatabaseGroup()->GetSubgroup(TABLE_LABEL, entry.RelativePath);

        return &entry;
    }

    void ForgetRoleBucketTablet(TTableEntry& entry, const TTabletKey& tablet) {
        auto& bucket = entry.GetRoleBucket(tablet.second);
        if (!bucket) {
            return;
        }

        bucket->Forget(tablet);

        if (bucket->IsEmpty()) {
            bucket.Reset();
            entry.TableGroup->RemoveSubgroup(ROLE_LABEL, GetRoleLabelValue(tablet.second));
        }
    }

    void ForgetLeaf(TTableEntry& entry, const TTabletKey& tablet) {
        if (!entry.Leaves.erase(tablet)) {
            return;
        }

        const auto& [tabletId, followerId] = tablet;

        const TString tabletIdValue = ToString(tabletId);

        auto tabletGroup = entry.PerPartitionGroup->FindSubgroup(TABLET_ID_LABEL, tabletIdValue);
        if (tabletGroup) {
            tabletGroup->RemoveSubgroup(FOLLOWER_ID_LABEL, ToString(followerId));
        }

        // The tablet group is removed as soon as its last follower is gone
        const bool hasOtherFollowers = AnyOf(entry.Leaves, [tabletId = tabletId](const auto& leaf) {
            return leaf.first.first == tabletId;
        });

        if (!hasOtherFollowers) {
            entry.PerPartitionGroup->RemoveSubgroup(TABLET_ID_LABEL, tabletIdValue);
        }

        if (entry.Leaves.empty()) {
            entry.PerPartitionGroup = nullptr;
            entry.TableGroup->RemoveSubgroup(DETAILED_METRICS_LABEL, PER_PARTITION_VALUE);
        }
    }

private:
    NMonitoring::TDynamicCounterPtr TargetCounterGroup;

    const TString DatabasePath;
    const TString MonitoringProjectId;

    /**
     * Created together with the very first table.
     */
    NMonitoring::TDynamicCounterPtr DatabaseGroup;

    THashMap<TPathId, TTableEntry> Tables;
};

} // namespace <anonymous>

TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    const TString& monitoringProjectId
) {
    return MakeIntrusive<TNodeDatabaseMetricsAggregatorImpl>(
        targetCounterGroup,
        databasePath,
        monitoringProjectId
    );
}

} // namespace NKikimr
