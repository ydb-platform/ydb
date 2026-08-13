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
const TString TABLE_LABEL = "table";
const TString DETAILED_METRICS_LABEL = "detailed_metrics";
const TString TABLET_ID_LABEL = "tablet_id";
const TString FOLLOWER_ID_LABEL = "follower_id";

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

/**
 * Strip the database path prefix from the full path of the table.
 */
TString MakeRelativeTablePath(const TString& databasePath, const TString& tablePath) {
    TStringBuf database(databasePath);
    database.ChopSuffix("/");

    TStringBuf relativePath(tablePath);

    // The "/" is required, so that /Root/db10/table is NOT stripped down to "0/table"
    // within the database /Root/db1
    if (relativePath.SkipPrefix(database) && relativePath.SkipPrefix("/") && !relativePath.empty()) {
        return TString(relativePath);
    }

    return tablePath;
}

/**
 * A single bucket of the detailed metrics counter tree: the low level counters
 * of one or more tablets of the same type.
 *
 * @note The bucket of a table level table holds many tablets, while a leaf group
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

        // TODO(djant) restrict the counter set
        // do NOT enable the Partition level in production.
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

    /**
     * The tablet type of the first tablet of this table that was registered.
     * All subsequent tablets of the same table must report the same type.
     */
    TTabletTypes::EType RegisteredTabletType = TTabletTypes::TypeInvalid;

    /**
     * Table level, created on demand: all same-node tablets of the table collapsed
     * into a single bucket, which lives directly in the table group.
     */
    THolder<TCountersBucket> TableBucket;

    // Partition level, created on demand
    NMonitoring::TDynamicCounterPtr PerPartitionGroup;
    THashMap<TTabletKey, THolder<TCountersBucket>> Leaves;

    bool IsEmpty() const {
        return !TableBucket && Leaves.empty();
    }
};

class TNodeDatabaseMetricsAggregatorImpl : public TNodeDatabaseMetricsAggregator {
public:
    TNodeDatabaseMetricsAggregatorImpl(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        bool isFollowerRole
    )
        : TargetCounterGroup(targetCounterGroup)
        , DatabasePath(databasePath)
        , IsFollowerRole(isFollowerRole)
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
        CheckSingleRole(followerId);

        const TTabletKey tablet(tabletId, followerId);

        // A tablet reports exactly one table, so a tablet, which is re-reported under
        // another one, leaves behind a contribution to the old table, which ForgetTablet
        // can no longer reach. Drop it here, BEFORE the group of the new table is created,
        // because dropping the last table of the database removes the database group too
        auto mapIt = TabletToTableMap.find(tablet);
        if (mapIt != TabletToTableMap.end() && mapIt->second != table.TableId) {
            RemoveTabletFromTable(mapIt->second, tablet);
            TabletToTableMap.erase(mapIt);
        }

        auto* entry = GetOrCreateTable(table);
        if (!entry) {
            return;
        }

        // Reject tablet type drift: all tablets of the same table must report the same type
        if (entry->RegisteredTabletType == TTabletTypes::TypeInvalid) {
            entry->RegisteredTabletType = tabletType;
        } else if (entry->RegisteredTabletType != tabletType) {
            Y_DEBUG_ABORT_UNLESS(
                false,
                "tablet %" PRIu64 " of table %s reports type %s but the table expects %s",
                tabletId,
                entry->Info.TablePath.c_str(),
                TTabletTypes::TypeToStr(tabletType),
                TTabletTypes::TypeToStr(entry->RegisteredTabletType)
            );

            // The aggregates of the bucket are built for the counter set of the registered
            // type, so feeding another layout into them aborts in TAggregatedTabletCounters
            return;
        }

        // Record the reverse mapping from tablet key to table for ForgetTablet
        TabletToTableMap[tablet] = table.TableId;

        if (IsTableLevel(entry->Info)) {
            auto& bucket = entry->TableBucket;
            if (!bucket) {
                bucket = MakeHolder<TCountersBucket>(entry->TableGroup, tabletType);
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

    void ForgetTablet(ui64 tabletId, ui32 followerId) override {
        const TTabletKey tablet(tabletId, followerId);

        auto mapIt = TabletToTableMap.find(tablet);
        if (mapIt == TabletToTableMap.end()) {
            // Unknown tablet: silent no-op, as per the contract
            return;
        }

        const TPathId tableId = mapIt->second;
        TabletToTableMap.erase(mapIt);

        RemoveTabletFromTable(tableId, tablet);
    }

    void RecalculateAllCounters() override {
        for (auto& [_, entry] : Tables) {
            if (entry.TableBucket) {
                entry.TableBucket->RecalcAll();
            }
            for (auto& [_, leaf] : entry.Leaves) {
                leaf->RecalcAll();
            }
        }
    }

private:
    /**
     * Assert that this instance is only ever handed the tablets of its own role.
     *
     * @note Both senders route by role
     *       (MakeTabletCountersAggregatorID(node, IsFollower()) in flat_executor.cpp
     *       and datashard.cpp), so a tablet of the other role means the wiring is
     *       broken. The Table level collapse cannot survive it: the leader-only public
     *       metrics are filtered by the mapper downstream, and once the roles are
     *       summed into one bucket there is nothing left to filter on.
     */
    void CheckSingleRole(ui32 followerId) const {
        Y_DEBUG_ABORT_UNLESS(
            IsFollowerRole == (followerId != 0),
            "the aggregator of the %s tablets got a follower ID of %" PRIu32,
            IsFollowerRole ? "follower" : "leader",
            followerId
        );
    }

    static bool IsTableLevel(const TDetailedMetricsTableInfo& table) {
        return table.MetricsLevel == TDetailedMetricsSettings::MetricsLevelTable;
    }

    static bool IsPartitionLevel(const TDetailedMetricsTableInfo& table) {
        return table.MetricsLevel == TDetailedMetricsSettings::MetricsLevelPartition;
    }

    NMonitoring::TDynamicCounterPtr GetOrCreateDatabaseGroup() {
        if (!DatabaseGroup) {
            DatabaseGroup = TargetCounterGroup->GetSubgroup(DATABASE_LABEL, DatabasePath);
        }

        return DatabaseGroup;
    }

    /**
     * Remove the database node from the target group. The next table recreates it.
     */
    void RemoveDatabaseGroup() {
        if (!DatabaseGroup) {
            return;
        }

        TargetCounterGroup->RemoveSubgroup(DATABASE_LABEL, DatabasePath);

        DatabaseGroup = nullptr;
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
        if (!IsTableLevel(table) && !IsPartitionLevel(table)) {
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
        //       change is implemented in a separate step (the level and rename step of
        //       the detailed metrics plan). Until then the level of a table is frozen at
        //       the very first report, which MetricsLevelChangeIsIgnoredUntilReconciliation
        //       pins, so that the step has to flip an explicit assertion

        auto& entry = Tables[table.TableId];
        entry.Info = table;
        entry.RelativePath = MakeRelativeTablePath(DatabasePath, table.TablePath);
        entry.TableGroup = GetOrCreateDatabaseGroup()->GetSubgroup(TABLE_LABEL, entry.RelativePath);

        return &entry;
    }

    /**
     * Drop the contribution of a single tablet to the given table, removing the groups,
     * which are left empty.
     *
     * @note The caller owns the reverse map entry: this function does not touch it.
     */
    void RemoveTabletFromTable(const TPathId& tableId, const TTabletKey& tablet) {
        auto it = Tables.find(tableId);
        if (it == Tables.end()) {
            // The table collects no detailed metrics, or its entry is already gone
            return;
        }

        auto& entry = it->second;

        if (IsTableLevel(entry.Info)) {
            ForgetTableBucketTablet(entry, tablet);
        } else {
            ForgetLeaf(entry, tablet);
        }

        if (entry.IsEmpty()) {
            DatabaseGroup->RemoveSubgroup(TABLE_LABEL, entry.RelativePath);
            Tables.erase(it);
        }

        // The database node is not kept around after its last table is gone: a node
        // stops hosting a database far more often than the process is restarted
        if (Tables.empty()) {
            RemoveDatabaseGroup();
        }
    }

    void ForgetTableBucketTablet(TTableEntry& entry, const TTabletKey& tablet) {
        auto& bucket = entry.TableBucket;
        if (!bucket) {
            return;
        }

        bucket->Forget(tablet);

        // The bucket lives directly in the table group, so its own counter groups are
        // dropped together with the table group by the caller, for which the emptied
        // entry is now IsEmpty()
        if (bucket->IsEmpty()) {
            bucket.Reset();
        }
    }

    void ForgetLeaf(TTableEntry& entry, const TTabletKey& tablet) {
        auto it = entry.Leaves.find(tablet);
        if (it == entry.Leaves.end()) {
            return;
        }

        // A leaf holds exactly this one tablet, so it is empty right afterwards. Dropping
        // the contribution first keeps this symmetric with the table bucket path and holds
        // even if a leaf ever comes to hold more than one tablet
        it->second->Forget(tablet);
        Y_DEBUG_ABORT_UNLESS(it->second->IsEmpty());

        entry.Leaves.erase(it);

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

    /**
     * The role of the tablets this instance serves. A validation input only: it never
     * reaches the counter tree, both roles build the very same shape.
     */
    const bool IsFollowerRole;

    /**
     * The database= node, where the table= nodes are created. Created together with
     * the very first table.
     */
    NMonitoring::TDynamicCounterPtr DatabaseGroup;

    /**
     * Reverse map from (tabletId, followerId) to tableId, used to satisfy ForgetTablet
     * when the forget event carries no table identity.
     */
    THashMap<TTabletKey, TPathId> TabletToTableMap;

    THashMap<TPathId, TTableEntry> Tables;
};

} // namespace <anonymous>

TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    bool isFollowerRole
) {
    return MakeIntrusive<TNodeDatabaseMetricsAggregatorImpl>(
        targetCounterGroup,
        databasePath,
        isFollowerRole
    );
}

} // namespace NKikimr
