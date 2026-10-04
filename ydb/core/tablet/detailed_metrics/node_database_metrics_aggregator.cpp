#include "node_database_metrics_aggregator.h"

#include "detailed_metrics_binding.h"
#include "detailed_metrics_tree.h"
#include "detailed_values_accumulator.h"
#include "memory_tags.h"

#include <ydb/core/sys_view/service/db_counters_codec.h>
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

        // Pending reports retain the absolute path even after its table entry is gone.
        using TContributionKey = std::pair<TString, TBucketKey>;

        struct TTabletInfo {
            TString RelativePath;
            EDetailedMetricsLevel Level;
        };

        /**
         * The final public metric values of a retired bucket (zero gauges, empty level histograms,
         * the deltas not reported yet), kept until the next Pack() reports them.
         */
        struct TRetiredBucket {
            TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
            NKikimrSysView::TDbCounters Final;
        };

        /**
         * Everything the aggregator keeps for a single table.
         *
         * @note Both shapes can be populated while a metrics level change converges
         *       tablet by tablet. Pack emits each populated shape under its own level.
         */
        struct TTableEntry {
            // Absolute path sent in reports; Tables itself is keyed by the relative path.
            TString TablePath;

            /**
             * The tablet type of the first tablet of this table that was registered.
             * All subsequent tablets of the same table must report the same type.
             */
            TTabletTypes::EType RegisteredTabletType = TTabletTypes::TypeInvalid;

            // Table level, created on demand: all same-node leaders of the table collapsed into a single bucket
            TMaybe<TDetailedValuesAccumulator> TableBucket;

            // Partition level, created on demand
            THashMap<TTabletKey, TDetailedValuesAccumulator> Leaves;

            bool IsEmpty() const {
                return !TableBucket && Leaves.empty();
            }
        };

        class TNodeDatabaseMetricsAggregatorImpl: public TNodeDatabaseMetricsAggregator {
        public:
            TNodeDatabaseMetricsAggregatorImpl(
                const TString& databasePath,
                bool isFollowerRole)
                : DatabasePath(databasePath)
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

                // A tablet re-reported under another table or level leaves a contribution to the old one, which ForgetTablet
                // can no longer reach: drop it before GetOrCreateTable(), as dropping it may erase the very entry
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

                    // The buckets of the table are bound to the counter layout of the registered type
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
                        bucket.ConstructInPlace(binding, false /* skipLeaderOnly */);
                    }
                    bucket->Apply(tablet, executorCounters, appCounters, now);
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
                        entry.TableBucket->Pack(*tableCounters->MutableTableMetrics());
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
                // Forget has removed the last source
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
             * @return The binding of the public metrics of the tablet type to the counter layout of its first
             *         report, or nullptr for a report of another layout, which is skipped (warned once)
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

                // A new entry: the one place the key is materialized into a TString.
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
                    ForgetTableBucketTablet(entry, tablet);
                } else {
                    ForgetLeaf(entry, tablet);
                }

                if (entry.IsEmpty()) {
                    Tables.erase(it);
                }
            }

            void ForgetTableBucketTablet(TTableEntry& entry, const TTabletKey& tablet) {
                auto& bucket = entry.TableBucket;
                if (!bucket) {
                    return;
                }

                bucket->Forget(tablet);

                if (bucket->IsEmpty()) {
                    DropTableBucket(entry);
                }
            }

            void DropTableBucket(TTableEntry& entry) {
                if (!entry.TableBucket) {
                    return;
                }

                const TTabletTypes::EType tabletType = entry.RegisteredTabletType;
                RetireBucket(entry.TablePath, Nothing(), tabletType, *entry.TableBucket);
                entry.TableBucket.Clear();
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
            const TString DatabasePath;

            /**
             * DatabasePath with the trailing "/" chopped, own storage (not a view into
             * DatabasePath): the impl is copy-constructible (TThrRefBase), and a view member
             * would alias the SOURCE's DatabasePath after a copy. Precomputed once so that
             * MakeRelativeTablePath() needs no allocation on every AddCounters call.
             */
            const TString DatabasePrefix;

            /**
             * The role of the tablets this instance serves. Only the leaders build TABLE buckets.
             */
            const bool IsFollowerRole;

            /**
             * Reverse map from (tabletId, followerId) to the table's relative path, used to
             * satisfy ForgetTablet when the forget event carries no table identity.
             */
            THashMap<TTabletKey, TTabletInfo> TabletToTableMap;

            /**
             * Keyed by the table's relative path rather than by TPathId: two PathIds sharing one path
             * (a table dropped and recreated at the same path, or an ESchemeOpMoveTable rename and a new
             * table at the vacated path) collapse into the very same entry, as Pack() reports a table by its path.
             */
            THashMap<TString, TTableEntry> Tables;

            // Outlives the live buckets until a report carries each retired bucket's final delta.
            THashMap<TContributionKey, TRetiredBucket> PendingCounters;

            // The buckets and the leaves point to the bindings, which are never destroyed while the instance lives
            THashMap<TTabletTypes::EType, THolder<TDetailedMetricsBinding>> Bindings;
            bool WarnedLayoutMismatch = false;
        };

    } // namespace

    TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
        const TString& databasePath,
        bool isFollowerRole) {
        NProfiling::TMemoryTagScope memoryScope(NDetailedMetrics::NodeMemoryTag());
        return MakeIntrusive<TNodeDatabaseMetricsAggregatorImpl>(
            databasePath,
            isFollowerRole);
    }

} // namespace NKikimr
