#include "node_database_metrics_aggregator.h"

#include "detailed_metrics_counter_set.h"
#include "detailed_metrics_tree.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

namespace NKikimr {

/**
 * Process-wide as there's only two TCA per node: leader and follower. Finer granularity will not buy anything.
 */
TMutex& DetailedMetricsLock() {
    static TMutex lock;
    return lock;
}

namespace {

struct TTabletInfo {
    TString RelativePath;
    EDetailedMetricsLevel Level;
};

/**
 * Everything the aggregator keeps for a single table.
 *
 * @note Only one of the two shapes is ever populated, the one chosen by
 *       the effective metrics level of the table.
 */
struct TTableEntry {
    NMonitoring::TDynamicCounterPtr TableGroup;

    /**
     * The identity Pack() reports this table under: the FULL path, the way the
     * tablets report it, not the relative path the entry is keyed by (the
     * processor strips the database prefix itself, off its own database path).
     */
    TString TablePath;

    /**
     * The level of the last tablet to report this table. Last writer wins: an
     * entry outlives a level change only while another tablet still holds it
     * (the changing tablet is moved out of the old shape by AddCounters), so
     * the freshest report is the one to publish.
     */
    EDetailedMetricsLevel MetricsLevel = TDetailedMetricsSettings::MetricsLevelUnspecified;

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
        const TString& tablePath,
        EDetailedMetricsLevel metricsLevel,
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
        const TStringBuf relativePath = MakeRelativeTablePath(DatabasePrefix, tablePath);

        // A tablet reports exactly one table, so a tablet, which is re-reported under
        // another one, leaves behind a contribution to the old table, which ForgetTablet
        // can no longer reach. Drop it here, BEFORE the group of the new table is created,
        // because dropping the last table of the database removes the database group too
        auto mapIt = TabletToTableMap.find(tablet);
        if (mapIt != TabletToTableMap.end()
            && (mapIt->second.RelativePath != relativePath || mapIt->second.Level != metricsLevel))
        {
            RemoveTabletFromTable(mapIt->second.RelativePath, tablet, mapIt->second.Level);
            TabletToTableMap.erase(mapIt);
            mapIt = TabletToTableMap.end();
        }

        if (IsFollowerRole && IsTableLevel(metricsLevel)) {
            return;
        }

        auto* entry = GetOrCreateTable(tablePath, metricsLevel, relativePath);
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
                tablePath.c_str(),
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
            TabletToTableMap.emplace(tablet, TTabletInfo{TString(relativePath), metricsLevel});
        }

        if (IsTableLevel(metricsLevel)) {
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
        RemoveTabletFromTable(mapIt->second.RelativePath, tablet, mapIt->second.Level);
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

    void Pack(NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out, ui64 generation) override {
        // Same lock as AddCounters/ForgetTablet: this is the writer<->reader axis
        // A2's note says step 07 must reuse (SharedTreeLock()/DetailedMetricsLock()),
        // guarding both the walk over Tables/Leaves below and every bucket's own
        // RecalcAll()/ToProto() republish window.
        TGuard<TMutex> guard(DetailedMetricsLock());

        for (auto& [relativePath, entry] : Tables) {
            auto* tableCounters = out.Add();
            tableCounters->SetTablePath(entry.TablePath);
            tableCounters->SetLevel(entry.MetricsLevel);

            if (entry.TableBucket) {
                entry.TableBucket->Pack(*tableCounters->MutableTableCounters(), generation);
            }

            for (auto& [tablet, leaf] : entry.Leaves) {
                auto* leafOut = tableCounters->AddLeaves();
                leafOut->SetTabletId(tablet.first);
                leafOut->SetFollowerId(tablet.second);
                leaf->Pack(*leafOut->MutableCounters(), generation);
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

    static bool IsTableLevel(EDetailedMetricsLevel level) {
        return level == TDetailedMetricsSettings::MetricsLevelTable;
    }

    static bool IsPartitionLevel(EDetailedMetricsLevel level) {
        return level == TDetailedMetricsSettings::MetricsLevelPartition;
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

    /**
     * @return The per-table state, or nullptr if the table collects no detailed metrics
     */
    TTableEntry* GetOrCreateTable(
        const TString& tablePath,
        EDetailedMetricsLevel metricsLevel,
        const TStringBuf relativePath
    ) {
        if (!IsTableLevel(metricsLevel) && !IsPartitionLevel(metricsLevel)) {
            return nullptr;
        }

        if (relativePath.empty()) {
            return nullptr;
        }

        // THash<TString>/TEqualTo<TString> are transparent, so lookup on a TStringBuf
        // needs no temporary TString
        auto it = Tables.find(relativePath);
        if (it != Tables.end()) {
            Y_DEBUG_ABORT_UNLESS(!it->second.IsEmpty());

            it->second.MetricsLevel = metricsLevel;

            return &it->second;
        }

        // A new entry: this is the one place the key is actually materialized into a
        // TString, once, shared between the map key and the GetSubgroup() call
        const TString newKey(relativePath);
        auto& entry = Tables[newKey];
        entry.TableGroup = GetOrCreateDatabaseGroup()->GetSubgroup(TABLE_LABEL, newKey);
        entry.TablePath = tablePath;
        entry.MetricsLevel = metricsLevel;

        return &entry;
    }

    void RemoveTabletFromTable(const TStringBuf relativePath, const TTabletKey& tablet, EDetailedMetricsLevel level) {
        auto it = Tables.find(relativePath);
        if (it == Tables.end()) {
            // The table collects no detailed metrics, or its entry is already gone
            return;
        }

        auto& entry = it->second;

        if (IsTableLevel(level)) {
            ForgetTableBucketTablet(it->first, entry, tablet);
        } else {
            ForgetLeaf(it->first, entry, tablet);
        }

        if (entry.IsEmpty()) {
            EraseTableEntry(it);
        }
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

        if (entry.Leaves.empty()) {
            entry.PerPartitionGroup.Reset();
        }
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
    THashMap<TTabletKey, TTabletInfo> TabletToTableMap;

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
