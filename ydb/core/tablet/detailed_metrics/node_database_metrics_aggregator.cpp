#include "node_database_metrics_aggregator.h"

#include "detailed_metrics_binding.h"
#include "detailed_metrics_tree.h"
#include "detailed_values_accumulator.h"
#include "memory_tags.h"

#include <ydb/core/sys_view/service/db_counters_codec.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/system/mutex.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TABLET_AGGREGATOR

namespace NKikimr {

    /**
     * Process-wide as there's only two TCA per node: leader and follower. Finer granularity will not buy anything.
     */
    TMutex& DetailedMetricsLock() {
        static TMutex lock;
        return lock;
    }

    namespace {

        using namespace NDetailedMetrics;

        // Pending reports retain the absolute path even after its published group is gone.
        using TContributionKey = std::pair<TString, TBucketKey>;

        struct TTabletInfo {
            TString RelativePath;
            EDetailedMetricsLevel Level;
        };

        /**
         * The final public metric values of a retired bucket.
         */
        struct TRetiredBucket {
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
            NKikimrSysView::TDbCounters Final;
        };

        /**
         * The TABLE bucket.
         *
         * @note A PARTITION leaf is a bare TDetailedValuesAccumulator (see TTableEntry::Leaves).
         */
        class TCountersBucket {
        public:
            TCountersBucket(
                NMonitoring::TDynamicCounterPtr bucketGroup,
                TTabletTypes::EType tabletType,
                NMonitoring::TCountableBase::EVisibility visibility,
                const TDetailedMetricsBinding& binding)
                : TabletType(tabletType)
                , TypeGroup(GetOrCreateTypeGroup(bucketGroup, tabletType))
                , ExecutorCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, EXECUTOR_CATEGORY), visibility)
                , AppCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, APP_CATEGORY), visibility)
                , Descriptor(binding.Descriptor)
                , Values(&binding, false /* skipLeaderOnly */)
            {
            }

            void Apply(
                const TTabletKey& tablet,
                const TTabletCountersBase& executorCounters,
                const TTabletCountersBase& appCounters,
                TInstant now) {
                // The aggregates identify their sources by a single ui64, while a bucket may hold
                // several followers of the same tablet, hence the synthetic source IDs
                auto [it, inserted] = SourceIds.try_emplace(tablet, NextSourceId);
                if (inserted) {
                    ++NextSourceId;
                }

                if (!ExecutorCounters.IsInitialized) {
                    ExecutorCounters.Initialize(&executorCounters, &Descriptor->ExecutorCounterNames);
                }
                if (!AppCounters.IsInitialized) {
                    AppCounters.Initialize(&appCounters, &Descriptor->AppCounterNames);
                }

                ExecutorCounters.Apply(it->second, &executorCounters, TabletType, now);
                AppCounters.Apply(it->second, &appCounters, TabletType, now);

                Values.Apply(tablet, executorCounters, appCounters, now);
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

                Values.Forget(tablet);
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

            TDetailedValuesAccumulator& GetValues() {
                return Values;
            }

            TTabletTypes::EType GetTabletType() const {
                return TabletType;
            }

        private:
            TTabletTypes::EType TabletType;

            NMonitoring::TDynamicCounterPtr TypeGroup;

            NPrivate::TAggregatedTabletCounters ExecutorCounters;
            NPrivate::TAggregatedTabletCounters AppCounters;

            // Static: its counter names select the counters the aggregates publish
            const TDetailedMetricsDescriptor* Descriptor;

            THashMap<TTabletKey, ui64> SourceIds;
            ui64 NextSourceId = 0;

            TDetailedValuesAccumulator Values;
        };

        /**
         * Everything the aggregator keeps for a single table.
         *
         * @note Both shapes can be populated while a metrics level change converges
         *       tablet by tablet. Pack emits each populated shape under its own level.
         */
        struct TTableEntry {
            NMonitoring::TDynamicCounterPtr TableGroup;
            // Absolute path sent in reports; Tables itself is keyed by the relative path.
            TString TablePath;

            /**
             * The tablet type of the first tablet of this table that was registered.
             * All subsequent tablets of the same table must report the same type.
             */
            TTabletTypes::EType RegisteredTabletType = TTabletTypes::TypeInvalid;

            /**
             * Table level, created on demand: all same-node leaders of the table collapsed
             * into a single bucket, which lives directly in the table group.
             */
            THolder<TCountersBucket> TableBucket;

            // Partition level, created on demand
            THashMap<TTabletKey, TDetailedValuesAccumulator> Leaves;

            bool IsEmpty() const {
                return !TableBucket && Leaves.empty();
            }
        };

        class TNodeDatabaseMetricsAggregatorImpl: public TNodeDatabaseMetricsAggregator {
        public:
            TNodeDatabaseMetricsAggregatorImpl(
                NMonitoring::TDynamicCounterPtr targetCounterGroup,
                const TString& databasePath,
                bool isFollowerRole)
                : TargetCounterGroup(targetCounterGroup)
                , CounterVisibility(targetCounterGroup->Visibility())
                , DatabasePath(databasePath)
                , DatabasePrefix(ChopTrailingSlash(databasePath))
                , IsFollowerRole(isFollowerRole)
            {
            }

            void AddCounters(
                const TString& tablePath,
                EDetailedMetricsLevel metricsLevel,
                ui64 tabletId,
                ui32 followerId,
                TTabletTypes::EType tabletType,
                const TTabletCountersBase& executorCounters,
                const TTabletCountersBase& appCounters,
                TInstant now) override {
                NProfiling::TMemoryTagScope memoryScope(NodeMemoryTag());
                TGuard<TMutex> guard(DetailedMetricsLock());

                CheckSingleRole(followerId);

                // The published set is a property of the tablet type: a type without one publishes nothing
                const TDetailedMetricsDescriptor* descriptor = GetDetailedMetricsDescriptor(tabletType);
                if (!descriptor) {
                    return;
                }

                const TDetailedMetricsBinding* binding = GetOrBind(*descriptor, executorCounters, appCounters);
                if (!binding) {
                    return;
                }

                const TTabletKey tablet(tabletId, followerId);
                const TStringBuf relativePath = MakeRelativeTablePath(DatabasePrefix, tablePath);

                // A tablet reports exactly one table, so a tablet, which is re-reported under
                // another one, leaves behind a contribution to the old table, which ForgetTablet
                // can no longer reach. Drop it here, BEFORE the group of the new table is created,
                // because dropping the last table of the database removes the database group too
                auto mapIt = TabletToTableMap.find(tablet);
                if (mapIt != TabletToTableMap.end() && (mapIt->second.RelativePath != relativePath || mapIt->second.Level != metricsLevel))
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
                        TTabletTypes::TypeToStr(entry->RegisteredTabletType));

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
                    if (!entry->TableBucket) {
                        CreateTableBucket(*entry, relativePath, tabletType, *binding);
                    }
                    entry->TableBucket->Apply(tablet, executorCounters, appCounters, now);
                } else {
                    auto [leaf, _] = entry->Leaves.try_emplace(tablet, binding, followerId != 0);
                    leaf->second.Apply(tablet, executorCounters, appCounters, now);
                }
            }

            void ForgetTablet(ui64 tabletId, ui32 followerId) override {
                NProfiling::TMemoryTagScope memoryScope(NodeMemoryTag());
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
                NProfiling::TMemoryTagScope memoryScope(NodeMemoryTag());
                // The guard is here  for the READER of the published counter VALUES
                // TAggregatedTabletCounters republishes every HIST(x) by clearing and
                // refilling it one tablet at a time
                TGuard<TMutex> guard(DetailedMetricsLock());

                for (auto& [_, entry] : Tables) {
                    if (entry.TableBucket) {
                        entry.TableBucket->RecalcAll();
                    }
                }
            }

            void Pack(NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out) override {
                NProfiling::TMemoryTagScope memoryScope(PayloadMemoryTag());
                TGuard<TMutex> guard(DetailedMetricsLock());
                const int firstAppendedTableIndex = out.size();

                for (auto& [_, entry] : Tables) {
                    if (entry.TableBucket) {
                        auto* tableCounters = out.Add();
                        tableCounters->SetTablePath(entry.TablePath);
                        tableCounters->SetLevel(TDetailedMetricsSettings::MetricsLevelTable);
                        tableCounters->SetTabletType(entry.RegisteredTabletType);
                        entry.TableBucket->GetValues().Pack(*tableCounters->MutableTableMetrics());
                    }

                    if (!entry.Leaves.empty()) {
                        auto* tableCounters = out.Add();
                        tableCounters->SetTablePath(entry.TablePath);
                        tableCounters->SetLevel(TDetailedMetricsSettings::MetricsLevelPartition);
                        tableCounters->SetTabletType(entry.RegisteredTabletType);

                        for (auto& [tablet, leaf] : entry.Leaves) {
                            auto* leafOut = tableCounters->AddLeaves();
                            leafOut->SetTabletId(tablet.first);
                            leafOut->SetFollowerId(tablet.second);
                            leaf.Pack(*leafOut->MutableMetrics());
                        }
                    }
                }

                if (!PendingCounters.empty()) {
                    AppendPendingCounters(out, firstAppendedTableIndex);
                }
            }

        private:
            void RetireBucket(
                const TString& tablePath, const TBucketKey& key, TTabletTypes::EType type, TDetailedValuesAccumulator& values)
            {
                NProfiling::TMemoryTagScope memoryScope(NodeMemoryTag());
                // Forget has removed the last source.
                NKikimrSysView::TDbCounters final;
                values.Pack(final);
                auto [it, inserted] = PendingCounters.try_emplace(TContributionKey{tablePath, key});
                if (!inserted) {
                    NProfiling::TMemoryTagScope payloadMemoryScope(PayloadMemoryTag());
                    NSysView::MergeCounterDeltas(final, it->second.Final);
                }
                it->second.Type = type;
                it->second.Final.Swap(&final);
            }

            void AppendPendingCounters(
                NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out, int firstAppendedTableIndex)
            {
                using TTableKey = std::pair<TString, EDetailedMetricsLevel>;
                THashMap<TTableKey, NKikimrSysView::TDetailedTableCounters*> tables;
                THashMap<TContributionKey, NKikimrSysView::TDbCounters*> buckets;
                // Index only this call's output: Pack appends to a caller-owned report.
                for (int i = firstAppendedTableIndex; i < out.size(); ++i) {
                    auto* table = out.Mutable(i);
                    tables.emplace(TTableKey{table->GetTablePath(), table->GetLevel()}, table);
                    if (table->HasTableMetrics()) {
                        buckets.emplace(TContributionKey{table->GetTablePath(), Nothing()}, table->MutableTableMetrics());
                    }
                    for (auto& leaf : *table->MutableLeaves()) {
                        buckets.emplace(TContributionKey{table->GetTablePath(), TTabletKey{leaf.GetTabletId(), leaf.GetFollowerId()}},
                                        leaf.MutableMetrics());
                    }
                }

                for (auto& [contribution, retired] : PendingCounters) {
                    if (auto it = buckets.find(contribution); it != buckets.end()) {
                        NSysView::MergeCounterDeltas(*it->second, retired.Final);
                        continue;
                    }
                    const auto& [path, key] = contribution;
                    const auto level = key ? TDetailedMetricsSettings::MetricsLevelPartition : TDetailedMetricsSettings::MetricsLevelTable;
                    auto& table = tables[TTableKey{path, level}];
                    if (!table) {
                        table = out.Add();
                        table->SetTablePath(path);
                        table->SetLevel(level);
                        table->SetTabletType(retired.Type);
                    }
                    if (key) {
                        auto* leaf = table->AddLeaves();
                        leaf->SetTabletId(key->first);
                        leaf->SetFollowerId(key->second);
                        leaf->MutableMetrics()->Swap(&retired.Final);
                    } else {
                        table->MutableTableMetrics()->Swap(&retired.Final);
                    }
                }
                PendingCounters.clear();
            }

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
                    followerId);
            }

            /**
             * @return The binding made by the first report of the tablet type, or nullptr for another layout
             */
            const TDetailedMetricsBinding* GetOrBind(
                const TDetailedMetricsDescriptor& descriptor,
                const TTabletCountersBase& executorCounters,
                const TTabletCountersBase& appCounters)
            {
                auto& binding = Bindings[descriptor.Type];
                if (!binding) {
                    binding = BindDetailedMetrics(descriptor, executorCounters, appCounters);
                    for (const auto& problem : binding->Problems) {
                        YDB_LOG_WARN("Problem with the detailed metrics binding of the tablet counters",
                            {"database", DatabasePath},
                            {"tabletType", TTabletTypes::TypeToStr(descriptor.Type)},
                            {"problem", problem});
                    }
                } else if (binding->LayoutSizes != TDetailedMetricsBinding::GetLayoutSizes(executorCounters, appCounters)) {
                    if (!WarnedLayoutMismatch) {
                        WarnedLayoutMismatch = true;
                        YDB_LOG_WARN("Skipped the detailed metrics of a tablet, whose counter layout differs from the one of its tablet type",
                            {"database", DatabasePath},
                            {"tabletType", TTabletTypes::TypeToStr(descriptor.Type)});
                    }
                    return nullptr;
                }
                return binding.Get();
            }

            static bool IsTableLevel(EDetailedMetricsLevel level) {
                return level == TDetailedMetricsSettings::MetricsLevelTable;
            }

            static bool IsPartitionLevel(EDetailedMetricsLevel level) {
                return level == TDetailedMetricsSettings::MetricsLevelPartition;
            }

            void CreateTableBucket(
                TTableEntry& entry,
                const TStringBuf relativePath,
                TTabletTypes::EType tabletType,
                const TDetailedMetricsBinding& binding)
            {
                entry.TableGroup = TargetCounterGroup
                    ->GetSubgroup(DATABASE_LABEL, DatabasePath)
                    ->GetSubgroup(TABLE_LABEL, TString(relativePath));
                entry.TableBucket = MakeHolder<TCountersBucket>(entry.TableGroup, tabletType, CounterVisibility, binding);
            }

            /**
             * @return The per-table state, or nullptr if the table collects no detailed metrics
             */
            TTableEntry* GetOrCreateTable(
                const TString& tablePath, EDetailedMetricsLevel metricsLevel, const TStringBuf relativePath)
            {
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

                    return &it->second;
                }

                auto& entry = Tables[TString(relativePath)];
                entry.TablePath = tablePath;

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
                    ForgetLeaf(entry, tablet);
                }

                if (entry.IsEmpty()) {
                    Tables.erase(it);
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
                RetireBucket(entry.TablePath, Nothing(), tabletType, entry.TableBucket->GetValues());
                entry.TableBucket.Reset();

                TargetCounterGroup->RemoveSubgroupChain(MakeRawBucketPath(Nothing(), tabletType, {
                                                                                                     {DATABASE_LABEL, DatabasePath},
                                                                                                     {TABLE_LABEL, relativePath},
                                                                                                 }));
                entry.TableGroup.Reset();
            }

            void ForgetLeaf(TTableEntry& entry, const TTabletKey& tablet) {
                auto it = entry.Leaves.find(tablet);
                if (it == entry.Leaves.end()) {
                    return;
                }

                // A leaf holds exactly this one tablet, so it is empty right afterwards. Dropping
                // the contribution first keeps this symmetric with the table bucket path and holds
                // even if a leaf ever comes to hold more than one tablet
                it->second.Forget(tablet);
                Y_DEBUG_ABORT_UNLESS(it->second.IsEmpty());

                RetireBucket(entry.TablePath, tablet, entry.RegisteredTabletType, it->second);
                entry.Leaves.erase(it);
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
             * The role of the tablets this instance serves: a follower instance never touches the counter tree.
             */
            const bool IsFollowerRole;

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

            // Outlives the live tree until a report carries each retired bucket's final delta.
            THashMap<TContributionKey, TRetiredBucket> PendingCounters;

            // The buckets and the leaves point to the bindings, so none is destroyed while the instance lives
            THashMap<TTabletTypes::EType, THolder<TDetailedMetricsBinding>> Bindings;
            bool WarnedLayoutMismatch = false;
        };

    } // namespace

    TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        bool isFollowerRole) {
        NProfiling::TMemoryTagScope memoryScope(NDetailedMetrics::NodeMemoryTag());
        return MakeIntrusive<TNodeDatabaseMetricsAggregatorImpl>(
            targetCounterGroup,
            databasePath,
            isFollowerRole);
    }

} // namespace NKikimr
