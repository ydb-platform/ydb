#include "node_database_metrics_aggregator.h"

#include "detailed_metrics_binding.h"
#include "detailed_metrics_descriptor.h"
#include "detailed_metrics_tree.h"
#include "detailed_values_accumulator.h"
#include "memory_tags.h"

#include <ydb/core/sys_view/service/db_counters_codec.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <utility>

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
         * The final public metric values of a retired bucket, kept until the next Pack()
         * reports them.
         */
        struct TRetiredBucket {
            /**
             * The tablet type, whose public metrics define the slots of Final.
             */
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;

            /**
             * Zero gauges, empty level histograms and the deltas, which are not reported yet.
             */
            NKikimrSysView::TDbCounters Final;
        };

        /**
         * The TABLE bucket of the detailed metrics: the leaders of a table level table,
         * all of the same type, collapsed.
         *
         * The bucket keeps two views of the same tablets:
         * - the public metric values (see TDetailedValuesAccumulator), which Pack() reports
         *   to the SysView Processor;
         * - the low level counters in the counter tree, a debug view refreshed only
         *   by RecalcAll().
         *
         * @note A PARTITION leaf has no such bucket: it is kept as its public metric values
         *       alone (see TTableEntry::Leaves).
         */
        class TCountersBucket {
        public:
            /**
             * @param[in] binding The binding of the public metrics of the tablet type to the counter
             *            layout of every tablet of the bucket, it must outlive the bucket
             *
             * @note Only the leaders reach a TABLE bucket (see AddCounters()), so every public
             *       metric is computed, the LeaderOnly ones included.
             */
            TCountersBucket(
                NMonitoring::TDynamicCounterPtr bucketGroup,
                TTabletTypes::EType tabletType,
                const TDetailedMetricsCounterNames& counterNames,
                NMonitoring::TCountableBase::EVisibility visibility,
                const TDetailedMetricsBinding& binding)
                : TabletType(tabletType)
                , TypeGroup(GetOrCreateTypeGroup(bucketGroup, tabletType))
                , ExecutorCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, EXECUTOR_CATEGORY), visibility)
                , AppCounters(TypeGroup->GetSubgroup(CATEGORY_LABEL, APP_CATEGORY), visibility)
                , CounterNames(&counterNames)
                , Binding(&binding)
                , Values(Binding, false /* skipLeaderOnly */)
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
                    ExecutorCounters.Initialize(&executorCounters, &CounterNames->ExecutorNames);
                }
                if (!AppCounters.IsInitialized) {
                    AppCounters.Initialize(&appCounters, &CounterNames->AppNames);
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
                Y_DEBUG_ABORT_UNLESS(SourceIds.empty() == Values.IsEmpty());
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

            /**
             * @return The public metric values of the bucket, which Pack() reports
             *         (see TDetailedValuesAccumulator::Pack())
             *
             * @note Packing them does not touch the low level counters of the counter tree:
             *       they are a debug view, which only RecalcAll() refreshes.
             */
            TDetailedValuesAccumulator& GetValues() {
                return Values;
            }

            TTabletTypes::EType GetTabletType() const {
                return TabletType;
            }

            /**
             * @return The binding, which every report applied to the bucket must match
             */
            const TDetailedMetricsBinding* GetBinding() const {
                return Binding;
            }

        private:
            TTabletTypes::EType TabletType;

            NMonitoring::TDynamicCounterPtr TypeGroup;

            NPrivate::TAggregatedTabletCounters ExecutorCounters;
            NPrivate::TAggregatedTabletCounters AppCounters;

            const TDetailedMetricsCounterNames* CounterNames;

            THashMap<TTabletKey, ui64> SourceIds;
            ui64 NextSourceId = 0;

            const TDetailedMetricsBinding* Binding;
            TDetailedValuesAccumulator Values;
        };

        /**
         * Everything the aggregator keeps for a single table.
         *
         * @note Both shapes can be populated while a metrics level change converges
         *       tablet by tablet. Pack emits each populated shape under its own level.
         */
        struct TTableEntry {
            /**
             * The table= group of the counter tree, which holds the low level counters
             * of TableBucket: created together with the bucket and released together with it,
             * so that a partition level table owns no counter group at all.
             */
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

            /**
             * Partition level, created on demand: a leaf per tablet, which is nothing but
             * the public metric values of that one tablet (a few hundred bytes), with no counter
             * group of its own.
             *
             * @note The leaves are kept by value: the accumulator is movable, and the binding
             *       it points to is owned by the aggregator and never moves.
             */
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

                // The public metrics are a property of the tablet type: a type without them publishes nothing
                const TDetailedMetricsDescriptor* descriptor = GetDetailedMetricsDescriptor(tabletType);
                if (!descriptor) {
                    return;
                }

                // Every counter layout of the type is bound on its first report
                const TDetailedMetricsBinding& binding = GetOrBind(*descriptor, executorCounters, appCounters);

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

                // The reverse mapping from tablet key to table for ForgetTablet is recorded once
                // the report is known to be applied. mapIt still points at an up to date entry
                // (found above and not the-erased-because-stale case), so the steady state —
                // every report but the first of a tablet — writes nothing and copies no string.
                const bool isRegistered = mapIt != TabletToTableMap.end();

                if (IsTableLevel(metricsLevel)) {
                    auto& bucket = entry->TableBucket;

                    // The same holds for another counter layout of the same type: an existing bucket
                    // is bound to the layout of its first report
                    if (bucket && bucket->GetBinding() != &binding) {
                        ReportBucketLayoutMismatch(tablePath, tabletId, followerId, tabletType);
                        return;
                    }

                    if (!isRegistered) {
                        RegisterTablet(tablet, relativePath, metricsLevel);
                    }

                    if (!bucket) {
                        CreateTableBucket(*entry, relativePath, tabletType, descriptor->RawNames, binding);
                    }
                    bucket->Apply(tablet, executorCounters, appCounters, now);
                } else {
                    // A leaf is the public metric values of its tablet alone: no counter group
                    auto [leaf, inserted] = entry->Leaves.try_emplace(tablet, &binding, followerId != 0);

                    // An existing leaf is bound to the layout of its first report as well
                    if (!inserted && leaf->second.GetBinding() != &binding) {
                        ReportBucketLayoutMismatch(tablePath, tabletId, followerId, tabletType);
                        return;
                    }

                    if (!isRegistered) {
                        RegisterTablet(tablet, relativePath, metricsLevel);
                    }

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
             *
             * @note Only the TABLE buckets have a counter tree: the walk is O(TABLE buckets),
             *       whatever the number of the PARTITION leaves.
             */
            void RecalculateAllCounters() override {
                NProfiling::TMemoryTagScope memoryScope(NodeMemoryTag());
                // The guard serializes the walk with Pack(), which runs on the SysView Service
                // thread. TAggregatedTabletCounters republishes every HIST(x) by clearing and
                // refilling it one tablet at a time, so only a reader, which holds the lock too,
                // never sees one half refilled
                TGuard<TMutex> guard(DetailedMetricsLock());

                for (auto& [_, entry] : Tables) {
                    if (entry.TableBucket) {
                        entry.TableBucket->RecalcAll();
                    }
                }
            }

            /**
             * Append the public metric values of every bucket: the live ones and the ones retired
             * since the previous call, each table entry tagged with the tablet type, whose public
             * metrics define the slots of its values.
             *
             * @note The low level counters of the counter tree are not recalculated here: they are
             *       a debug view, which only RecalculateAllCounters() refreshes.
             */
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
            /**
             * Keep the final public metric values of a bucket (a TABLE bucket or a PARTITION leaf)
             * until the next Pack() reports them.
             *
             * @param[in] type The tablet type of the bucket, whose public metrics define the slots
             *            of the values
             * @param[in] values The values of the bucket, whose last source is forgotten
             */
            void RetireBucket(
                const TString& tablePath,
                const TBucketKey& key,
                TTabletTypes::EType type,
                TDetailedValuesAccumulator& values)
            {
                NProfiling::TMemoryTagScope memoryScope(NodeMemoryTag());
                // Forget has removed the last source. The final values hold the deltas, which are
                // not reported yet, the gauges zero and the level histograms empty, so that the
                // receiver retires the observations of the bucket.
                NKikimrSysView::TDbCounters final;
                values.Pack(final);
                auto [it, inserted] = PendingCounters.try_emplace(TContributionKey{tablePath, key});
                auto& retired = it->second;
                // The bucket was retired before, and its final values are not reported yet: the deltas
                // of both add up, the newest gauges and level histograms win. The values of another
                // tablet type (a table recreated at the same path) do not fit the slots of this one,
                // so the newest values replace them
                if (!inserted && retired.Type == type) {
                    NProfiling::TMemoryTagScope payloadMemoryScope(PayloadMemoryTag());
                    NSysView::MergeCounterDeltas(final, retired.Final);
                }
                retired.Type = type;
                retired.Final.Swap(&final);
            }

            void AppendPendingCounters(
                NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out, int firstAppendedTableIndex)
            {
                using TTableKey = std::pair<TString, EDetailedMetricsLevel>;
                THashMap<TTableKey, NKikimrSysView::TDetailedTableCounters*> tables;
                THashMap<TContributionKey, std::pair<TTabletTypes::EType, NKikimrSysView::TDbCounters*>> buckets;
                // Index only this call's output: Pack appends to a caller-owned report.
                for (int i = firstAppendedTableIndex; i < out.size(); ++i) {
                    auto* table = out.Mutable(i);
                    const TTabletTypes::EType type = table->GetTabletType();
                    tables.emplace(TTableKey{table->GetTablePath(), table->GetLevel()}, table);
                    if (table->HasTableMetrics()) {
                        buckets.emplace(TContributionKey{table->GetTablePath(), Nothing()},
                                        std::make_pair(type, table->MutableTableMetrics()));
                    }
                    for (auto& leaf : *table->MutableLeaves()) {
                        buckets.emplace(TContributionKey{table->GetTablePath(), TTabletKey{leaf.GetTabletId(), leaf.GetFollowerId()}},
                                        std::make_pair(type, leaf.MutableMetrics()));
                    }
                }

                // The values of a retired bucket fit only an entry of the same tablet type. An entry
                // of another type (a table recreated at the same path) supersedes them
                for (auto& [contribution, retired] : PendingCounters) {
                    if (auto it = buckets.find(contribution); it != buckets.end()) {
                        const auto& [type, values] = it->second;
                        if (type == retired.Type) {
                            NSysView::MergeCounterDeltas(*values, retired.Final);
                        }
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
                    } else if (table->GetTabletType() != retired.Type) {
                        continue;
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
             * Get the binding of the public metrics of the tablet type to the counter layout
             * of the report, binding the layout on its first report.
             *
             * Every tablet of a type normally reports one and the same layout, which is bound
             * once. Another layout of the same type does not match that binding: it gets its own
             * binding, found by the signature of the layout as seen by the first binding.
             *
             * @note That signature covers only the slots, which the first binding reads, so
             *       different layouts may share it: the bindings of one signature are told apart
             *       by Matches().
             *
             * @return The binding, which matches the layout
             */
            const TDetailedMetricsBinding& GetOrBind(
                const TDetailedMetricsDescriptor& descriptor,
                const TTabletCountersBase& executorCounters,
                const TTabletCountersBase& appCounters)
            {
                auto& binding = Bindings[descriptor.Type];
                if (!binding) {
                    binding = Bind(descriptor, executorCounters, appCounters);
                    return *binding;
                }

                if (binding->Matches(executorCounters, appCounters)) {
                    return *binding;
                }

                auto& extras = ExtraBindings[std::make_pair(descriptor.Type, binding->GetLayoutSignature(executorCounters, appCounters))];
                for (const auto& extra : extras) {
                    if (extra->Matches(executorCounters, appCounters)) {
                        return *extra;
                    }
                }

                YDB_LOG_WARN("Another counter layout of the tablet type gets its own detailed metrics binding",
                    {"database", DatabasePath},
                    {"tabletType", TTabletTypes::TypeToStr(descriptor.Type)});
                extras.push_back(Bind(descriptor, executorCounters, appCounters));
                return *extras.back();
            }

            /**
             * Bind the descriptor to the given layout, reporting the problems of the binding once.
             */
            THolder<TDetailedMetricsBinding> Bind(
                const TDetailedMetricsDescriptor& descriptor,
                const TTabletCountersBase& executorCounters,
                const TTabletCountersBase& appCounters) const
            {
                auto binding = BindDetailedMetrics(descriptor, executorCounters, appCounters);
                for (const auto& problem : binding->Problems) {
                    YDB_LOG_WARN("Problem with the detailed metrics binding of the tablet counters",
                        {"database", DatabasePath},
                        {"tabletType", TTabletTypes::TypeToStr(descriptor.Type)},
                        {"problem", problem});
                }
                return binding;
            }

            /**
             * Report a tablet, whose counter layout differs from the one of the bucket
             * it reports to (once per instance): its report is skipped.
             */
            void ReportBucketLayoutMismatch(
                const TString& tablePath, ui64 tabletId, ui32 followerId, TTabletTypes::EType tabletType)
            {
                if (WarnedBucketLayoutMismatch) {
                    return;
                }
                WarnedBucketLayoutMismatch = true;
                YDB_LOG_CRIT("A tablet reports another counter layout than the detailed metrics bucket it belongs to, the report is skipped",
                    {"database", DatabasePath},
                    {"tablePath", tablePath},
                    {"tabletId", tabletId},
                    {"followerId", followerId},
                    {"tabletType", TTabletTypes::TypeToStr(tabletType)});
            }

            static bool IsTableLevel(EDetailedMetricsLevel level) {
                return level == TDetailedMetricsSettings::MetricsLevelTable;
            }

            static bool IsPartitionLevel(EDetailedMetricsLevel level) {
                return level == TDetailedMetricsSettings::MetricsLevelPartition;
            }

            void RegisterTablet(const TTabletKey& tablet, const TStringBuf relativePath, EDetailedMetricsLevel metricsLevel) {
                TabletToTableMap.emplace(tablet, TTabletInfo{TString(relativePath), metricsLevel});
            }

            /**
             * Create the TABLE bucket of the table together with the groups of the counter tree,
             * which hold its low level counters: database= and table= are created on demand
             * by the very first bucket under them, and removed by the last one (see DropTableBucket()).
             */
            void CreateTableBucket(
                TTableEntry& entry,
                const TStringBuf relativePath,
                TTabletTypes::EType tabletType,
                const TDetailedMetricsCounterNames& counterNames,
                const TDetailedMetricsBinding& binding)
            {
                Y_DEBUG_ABORT_UNLESS(!entry.TableBucket && !entry.TableGroup);

                entry.TableGroup = TargetCounterGroup
                    ->GetSubgroup(DATABASE_LABEL, DatabasePath)
                    ->GetSubgroup(TABLE_LABEL, TString(relativePath));
                entry.TableBucket = MakeHolder<TCountersBucket>(
                    entry.TableGroup,
                    tabletType,
                    counterNames,
                    CounterVisibility,
                    binding);
            }

            /**
             * @return The per-table state, or nullptr if the table collects no detailed metrics
             *
             * @note A new entry creates no counter group: only its TABLE bucket does.
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

                // A new entry: the only report, whose key is materialized into a TString for the map
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

            /**
             * Retire the TABLE bucket of the table and remove its groups from the counter tree:
             * its type= group, and the table= and database= groups above it, which it leaves empty.
             */
            void DropTableBucket(const TString& relativePath, TTableEntry& entry) {
                if (!entry.TableBucket) {
                    return;
                }

                const TTabletTypes::EType tabletType = entry.TableBucket->GetTabletType();
                RetireBucket(entry.TablePath, Nothing(), tabletType, entry.TableBucket->GetValues());
                entry.TableBucket.Reset();

                TargetCounterGroup->RemoveSubgroupChain(MakeRawBucketPath(tabletType, {
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
             * The role of the tablets this instance serves. Only the leaders build TABLE buckets,
             * so the instance of the followers keeps PARTITION leaves alone and never touches
             * the counter tree.
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

            /**
             * The binding of the public metrics of every tablet type to the counter layout
             * of its first report. The TABLE buckets and the PARTITION leaves point to the bindings,
             * so they are never destroyed or moved (THolder) while the instance lives.
             */
            THashMap<TTabletTypes::EType, THolder<TDetailedMetricsBinding>> Bindings;

            /**
             * The bindings of the other counter layouts of a tablet type, keyed by the type
             * and by the signature of the layout as seen by its binding in Bindings: one binding
             * per layout, several layouts may share a key.
             */
            THashMap<std::pair<TTabletTypes::EType, TDetailedMetricsLayoutSignature>, TVector<THolder<TDetailedMetricsBinding>>> ExtraBindings;

            bool WarnedBucketLayoutMismatch = false;
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
