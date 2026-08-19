#include "node_database_metrics_aggregator.h"

#include "detailed_metrics_counter_set.h"

#include <ydb/core/tablet/private/aggregated_tablet_counters.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

#include <tuple>

namespace NKikimr {

/**
 * Process-wide as there's only two TCA per node: leader and follower. Finer granularity will not buy anything.
 */
TMutex& DetailedMetricsLock() {
    static TMutex lock;
    return lock;
}

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
 * @return path with any trailing "/" chopped, as a view into path.
 */
TStringBuf ChopTrailingSlash(const TStringBuf path) {
    TStringBuf chopped(path);
    chopped.ChopSuffix("/");
    return chopped;
}

/**
 * Strip the database path prefix from the full path of the table.
 *
 * @param[in] databasePrefix The database path with the trailing "/" already chopped
 *                            (see TNodeDatabaseMetricsAggregatorImpl::DatabasePrefix)
 * @param[in] tablePath The full path of the table, which outlives the returned view
 *
 * @return A view into tablePath: either the stripped suffix, or tablePath itself
 *         when it does not start with the database. No allocation either way.
 */
TStringBuf MakeRelativeTablePath(const TStringBuf databasePrefix, const TString& tablePath) {
    TStringBuf relativePath(tablePath);

    // The "/" is required, so that /Root/db10/table is NOT stripped down to "0/table"
    // within the database /Root/db1
    if (relativePath.SkipPrefix(databasePrefix) && relativePath.SkipPrefix("/") && !relativePath.empty()) {
        return relativePath;
    }

    return TStringBuf(tablePath);
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
    TCountersBucket(
        NMonitoring::TDynamicCounterPtr bucketGroup,
        TTabletTypes::EType tabletType,
        const TDetailedMetricsCounterNames& counterNames,
        NMonitoring::TCountableBase::EVisibility visibility
    )
        : TabletType(tabletType)
        , TypeGroup(bucketGroup->GetSubgroup(TYPE_LABEL, TTabletTypes::TypeToStr(tabletType)))
        , ExecutorCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, EXECUTOR_CATEGORY), visibility)
        , AppCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, APP_CATEGORY), visibility)
        , CounterNames(&counterNames)
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

        if (!ExecutorCounters.IsInitialized) {
            ExecutorCounters.Initialize(&executorCounters, &CounterNames->ExecutorNames);
        }
        if (!AppCounters.IsInitialized) {
            AppCounters.Initialize(&appCounters, &CounterNames->AppNames);
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

    const TDetailedMetricsCounterNames* CounterNames;

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
        , CounterVisibility(targetCounterGroup->Visibility())
        , DatabasePath(databasePath)
        , DatabasePrefix(ChopTrailingSlash(databasePath))
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
        TGuard<TMutex> guard(DetailedMetricsLock());

        CheckSingleRole(followerId);

        // The published set is a property of the tablet type: a type without one publishes nothing
        const TDetailedMetricsCounterNames* counterNames = GetDetailedMetricsCounterNames(tabletType);
        if (!counterNames) {
            return;
        }

        const TTabletKey tablet(tabletId, followerId);
        const TStringBuf relativePath = MakeRelativeTablePath(DatabasePrefix, table.TablePath);

        // A tablet reports exactly one table, so a tablet, which is re-reported under
        // another one, leaves behind a contribution to the old table, which ForgetTablet
        // can no longer reach. Drop it here, BEFORE the group of the new table is created,
        // because dropping the last table of the database removes the database group too
        auto mapIt = TabletToTableMap.find(tablet);
        if (mapIt != TabletToTableMap.end() && mapIt->second != relativePath) {
            RemoveTabletFromTable(mapIt->second, tablet);
            TabletToTableMap.erase(mapIt);
            mapIt = TabletToTableMap.end();
        }

        ReconcileTable(relativePath, table);

        if (IsFollowerRole && IsTableLevel(table)) {
            return;
        }

        auto* entry = GetOrCreateTable(table, relativePath);
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

        // Record the reverse mapping from tablet key to table for ForgetTablet. mapIt
        // still points at an up to date entry (found above and not the-erased-because-
        // stale case), so the steady state — every report but the first of a tablet —
        // writes nothing and copies no string.
        if (mapIt == TabletToTableMap.end()) {
            TabletToTableMap.emplace(tablet, TString(relativePath));
        }

        if (IsTableLevel(entry->Info)) {
            auto& bucket = entry->TableBucket;
            if (!bucket) {
                bucket = MakeHolder<TCountersBucket>(
                    entry->TableGroup,
                    tabletType,
                    *counterNames,
                    CounterVisibility
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
                    tabletType,
                    *counterNames,
                    CounterVisibility
                );
            }
            leaf->Apply(tablet, executorCounters, appCounters, now);
        }
    }

    void ForgetTablet(ui64 tabletId, ui32 followerId) override {
        TGuard<TMutex> guard(DetailedMetricsLock());

        const TTabletKey tablet(tabletId, followerId);

        auto mapIt = TabletToTableMap.find(tablet);
        if (mapIt == TabletToTableMap.end()) {
            // Unknown tablet: silent no-op, as per the contract
            return;
        }

        // RemoveTabletFromTable takes relativePath as a view rather than copying it, so
        // it MUST run before the reverse map entry it points into is erased below, or the
        // view dangles. RemoveTabletFromTable is documented not to touch the reverse map,
        // so calling it first before this function's own erase is safe.
        RemoveTabletFromTable(mapIt->second, tablet);
        TabletToTableMap.erase(mapIt);
    }

    /**
     * Republish every aggregate of the tree, taking DetailedMetricsLock() for the whole
     * walk. See the lock's own comment for what it does and does not cover.
     */
    void RecalculateAllCounters() override {
        // The guard is here  for the READER of the published counter VALUES
        // TAggregatedTabletCounters republishes every HIST(x) by clearing and
        // refilling it one tablet at a time
        TGuard<TMutex> guard(DetailedMetricsLock());

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

    NMonitoring::TDynamicCounterPtr GetOrCreatePerPartitionGroup(TTableEntry& entry) {
        if (!entry.PerPartitionGroup) {
            entry.PerPartitionGroup = entry.TableGroup->GetSubgroup(
                DETAILED_METRICS_LABEL,
                PER_PARTITION_VALUE
            );
        }

        return entry.PerPartitionGroup;
    }

    void ReconcileTable(const TStringBuf relativePath, const TDetailedMetricsTableInfo& table) {
        auto it = Tables.find(relativePath);
        if (it == Tables.end()) {
            return;
        }

        const TDetailedMetricsTableInfo& stored = it->second.Info;

        if (table.MetricsLevel != stored.MetricsLevel) {
            DropTableEntry(it);
            return;
        }

        // A table recreated at this path restarts SchemaVersion low, so a plain
        // SchemaVersion comparison would pin Info to the older, already deleted table
        // forever. A recreated table always gets a newer PathId, so the identity is
        // ordered by the PathId first, and only then by SchemaVersion within one PathId.
        if (std::tie(table.TableId, table.SchemaVersion) > std::tie(stored.TableId, stored.SchemaVersion)) {
            it->second.Info = table;
        }
    }

    /**
     * @return The per-table state, or nullptr if the table collects no detailed metrics
     */
    TTableEntry* GetOrCreateTable(const TDetailedMetricsTableInfo& table, const TStringBuf relativePath) {
        if (!IsTableLevel(table) && !IsPartitionLevel(table)) {
            return nullptr;
        }

        if (!table.TableId || !table.TablePath) {
            return nullptr;
        }

        // THash<TString>/TEqualTo<TString> are transparent, so lookup on a TStringBuf
        // needs no temporary TString
        auto it = Tables.find(relativePath);
        if (it != Tables.end()) {
            Y_DEBUG_ABORT_UNLESS(!it->second.IsEmpty());

            return &it->second;
        }

        // A new entry: this is the one place the key is actually materialized into a
        // TString, once, shared between the map key and the GetSubgroup() call
        const TString newKey(relativePath);
        auto& entry = Tables[newKey];
        entry.Info = table;
        entry.TableGroup = GetOrCreateDatabaseGroup()->GetSubgroup(TABLE_LABEL, newKey);

        return &entry;
    }

    void RemoveTabletFromTable(const TStringBuf relativePath, const TTabletKey& tablet) {
        auto it = Tables.find(relativePath);
        if (it == Tables.end()) {
            // The table collects no detailed metrics, or its entry is already gone
            return;
        }

        auto& entry = it->second;

        if (IsTableLevel(entry.Info)) {
            ForgetTableBucketTablet(it->first, entry, tablet);
        } else {
            ForgetLeaf(it->first, entry, tablet);
        }

        if (entry.IsEmpty()) {
            EraseTableEntry(it);
        }
    }

    void DropTableEntry(THashMap<TString, TTableEntry>::iterator it) {
        auto& entry = it->second;

        TVector<TTabletKey> tablets;
        tablets.reserve(entry.Leaves.size());
        for (const auto& [tablet, _] : entry.Leaves) {
            tablets.push_back(tablet);
        }

        for (const auto& tablet : tablets) {
            ForgetLeaf(it->first, entry, tablet);
        }

        DropTableBucket(it->first, entry);

        Y_DEBUG_ABORT_UNLESS(entry.IsEmpty());

        EraseTableEntry(it);
    }

    void EraseTableEntry(THashMap<TString, TTableEntry>::iterator it) {
        Tables.erase(it);

        if (Tables.empty()) {
            DatabaseGroup.Reset();
        }
    }

    void ForgetTableBucketTablet(
        const TString& relativePath, TTableEntry& entry, const TTabletKey& tablet)
    {
        auto& bucket = entry.TableBucket;
        if (!bucket) {
            return;
        }

        bucket->Forget(tablet);

        if (bucket->IsEmpty()) {
            DropTableBucket(relativePath, entry);
        }
    }

    void DropTableBucket(const TString& relativePath, TTableEntry& entry) {
        if (!entry.TableBucket) {
            return;
        }

        const TTabletTypes::EType tabletType = entry.RegisteredTabletType;
        entry.TableBucket.Reset();

        TargetCounterGroup->RemoveSubgroupChain({
            {DATABASE_LABEL, DatabasePath},
            {TABLE_LABEL, relativePath},
            {TYPE_LABEL, TTabletTypes::TypeToStr(tabletType)},
        });
    }

    void ForgetLeaf(const TString& relativePath, TTableEntry& entry, const TTabletKey& tablet) {
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

        TargetCounterGroup->RemoveSubgroupChain({
            {DATABASE_LABEL, DatabasePath},
            {TABLE_LABEL, relativePath},
            {DETAILED_METRICS_LABEL, PER_PARTITION_VALUE},
            {TABLET_ID_LABEL, ToString(tabletId)},
            {FOLLOWER_ID_LABEL, ToString(followerId)},
        });
    }

private:
    NMonitoring::TDynamicCounterPtr TargetCounterGroup;

    const NMonitoring::TCountableBase::EVisibility CounterVisibility;

    const TString DatabasePath;

    /**
     * DatabasePath with the trailing "/" chopped, own storage (not a view into
     * DatabasePath): the impl is copy-constructible (TThrRefBase), and a view member
     * would alias the SOURCE's DatabasePath after a copy. Precomputed once so that
     * MakeRelativeTablePath() needs no allocation on every AddCounters call.
     */
    const TString DatabasePrefix;

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
     * Reverse map from (tabletId, followerId) to the table's relative path, used to
     * satisfy ForgetTablet when the forget event carries no table identity.
     */
    THashMap<TTabletKey, TString> TabletToTableMap;

    /**
     * Keyed by the table's relative path (the same value the "table" label of the
     * counter tree carries) rather than by TPathId.
     *
     * The counter group is created by GetSubgroup(TABLE_LABEL, relativePath), which
     * returns the SAME group for any two calls with the same path. If the state were
     * keyed by TPathId instead, two PathIds sharing one path — a table dropped and
     * recreated at the same path, or an ESchemeOpMoveTable rename that moves a table
     * away and a new one is created at the vacated path — would get two entries
     * silently aliasing one TDynamicCounters group and one implicit source-ID space.
     * Emptying either entry would then remove the group out from under the other, and
     * their independent TAggregatedTabletCounters would keep overwriting each other's
     * sums. Keying by path instead makes the two reports collapse into the very same
     * entry, which is exactly what the shared group already does.
     */
    THashMap<TString, TTableEntry> Tables;
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
