#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_counter_set.h"
#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/table_metrics_settings.pb.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {

namespace {

using TMetricsSettings = NKikimrSchemeOp::TTableDetailedMetricsSettings;
using EMetricsLevel = TMetricsSettings::EMetricsLevel;

// ---- Ported from ydb/core/sys_view/processor/db_counters.cpp (S2), scoped
//      down from the TDbServiceCounters/repeated-TabletCounters funnel to a
//      single NKikimrSysView::TDbTabletCounters: this class is already "per
//      table/per leaf", one level above where db_counters.cpp is "per
//      service" — TCrossNodeEntry below is what db_counters.cpp's
//      NodeCountersStates/AggregatedCountersState pair is to a service.
//      db_counters.cpp itself is untouched: this is a separate funnel (the
//      detailed metrics one, step 08+), not on the same release cadence.

template <bool IsMax>
struct TAggregateCumulative {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        auto cumulativeSize = src.GetCumulativeCount();
        auto histogramSize = src.HistogramSize();

        if (dst->CumulativeSize() < cumulativeSize) {
            dst->MutableCumulative()->Resize(cumulativeSize, 0);
        }
        if (dst->HistogramSize() < histogramSize) {
            auto missing = histogramSize - dst->HistogramSize();
            for (; missing > 0; --missing) {
                dst->AddHistogram();
            }
        }

        const auto& from = src.GetCumulative();
        auto* to = dst->MutableCumulative();
        auto doubleDiffSize = from.size() / 2 * 2;
        for (int i = 0; i < doubleDiffSize; ) {
            auto index = from[i++];
            auto value = from[i++];
            if (index >= cumulativeSize) {
                continue;
            }
            if constexpr (!IsMax) {
                (*to)[index] += value;
            } else {
                (*to)[index] = std::max(value, (*to)[index]);
            }
        }
        for (size_t i = 0; i < histogramSize; ++i) {
            const auto& histogram = src.GetHistogram(i);
            const auto& from = histogram.GetBuckets();
            auto* to = dst->MutableHistogram(i)->MutableBuckets();
            auto bucketCount = histogram.GetBucketsCount();
            if (to->size() < (int)bucketCount) {
                to->Resize(bucketCount, 0);
            }
            auto doubleDiffSize = from.size();
            for (int b = 0; b < doubleDiffSize; ) {
                auto index = from[b++];
                auto value = from[b++];
                if (index >= bucketCount) {
                    continue;
                }
                if constexpr (!IsMax) {
                    (*to)[index] += value;
                } else {
                    (*to)[index] = std::max(value, (*to)[index]);
                }
            }
        }
    }
};

template <bool IsMax>
struct TAggregateSimple {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        auto simpleSize = src.SimpleSize();
        if (dst->SimpleSize() < simpleSize) {
            dst->MutableSimple()->Resize(simpleSize, 0);
        }
        const auto& from = src.GetSimple();
        auto* to = dst->MutableSimple();
        for (size_t i = 0; i < simpleSize; ++i) {
            if constexpr (!IsMax) {
                (*to)[i] += from[i];
            } else {
                (*to)[i] = std::max(from[i], (*to)[i]);
            }
        }
    }
};

void ResetSimpleCounters(NKikimrSysView::TDbCounters* dst) {
    auto simpleSize = dst->SimpleSize();
    auto* to = dst->MutableSimple();
    for (size_t i = 0; i < simpleSize; ++i) {
        (*to)[i] = 0;
    }
}

void ResetMaxCounters(NKikimrSysView::TDbCounters* dst) {
    ResetSimpleCounters(dst);
    auto cumulativeSize = dst->CumulativeSize();
    auto* to = dst->MutableCumulative();
    for (size_t i = 0; i < cumulativeSize; ++i) {
        (*to)[i] = 0;
    }
}

/**
 * Sum the Cumulative/HIST delta of one node's report into the cross-node
 * accumulator (AggregateIncrementalCounters convention, S2). Max has no
 * meaningful incremental delta: the cross-node MAX is recomputed statefully
 * instead, from every live node's latest snapshot, see
 * AggregateStatefulTabletCounters/TCrossNodeEntry::Recalculate below.
 */
void AggregateIncrementalTabletCounters(
    NKikimrSysView::TDbTabletCounters* dst,
    const NKikimrSysView::TDbTabletCounters& src
) {
    TAggregateCumulative<false>::Apply(dst->MutableExecutorCounters(), src.GetExecutorCounters());
    TAggregateCumulative<false>::Apply(dst->MutableAppCounters(), src.GetAppCounters());
}

/**
 * Sum one node's latest Simple snapshot and MAX (both its Simple and its
 * Cumulative per-second-rate parts) into the cross-node accumulator
 * (AggregateStatefulCounters convention, S2).
 */
void AggregateStatefulTabletCounters(
    NKikimrSysView::TDbTabletCounters* dst,
    const NKikimrSysView::TDbTabletCounters& src
) {
    TAggregateSimple<false>::Apply(dst->MutableExecutorCounters(), src.GetExecutorCounters());
    TAggregateSimple<false>::Apply(dst->MutableAppCounters(), src.GetAppCounters());

    TAggregateSimple<true>::Apply(dst->MutableMaxExecutorCounters(), src.GetMaxExecutorCounters());
    TAggregateCumulative<true>::Apply(dst->MutableMaxExecutorCounters(), src.GetMaxExecutorCounters());
    TAggregateSimple<true>::Apply(dst->MutableMaxAppCounters(), src.GetMaxAppCounters());
    TAggregateCumulative<true>::Apply(dst->MutableMaxAppCounters(), src.GetMaxAppCounters());
}

void ResetStatefulTabletCounters(NKikimrSysView::TDbTabletCounters* dst) {
    ResetSimpleCounters(dst->MutableExecutorCounters());
    ResetSimpleCounters(dst->MutableAppCounters());
    ResetMaxCounters(dst->MutableMaxExecutorCounters());
    ResetMaxCounters(dst->MutableMaxAppCounters());
}

/**
 * The cross-node accumulator of one bucket — a TABLE-level table's collapse,
 * or a single PARTITION leaf. Per-node Simple/MAX state is cleared and
 * replaced on every ApplyDelta() (C2: the receiver-clears-per-node-state
 * trick), Cumulative/HIST deltas are summed straight into Accumulator as
 * they arrive; Recalculate() re-derives Accumulator's Simple/MAX part from
 * scratch across the currently live nodes and hands the result to a
 * TCountersBucket to publish.
 *
 * @note Used identically for a TABLE bucket (many nodes, exactly one per
 *       node since S1'' gives it a single leader-side source) and for a
 *       PARTITION leaf (normally one contributing node — the tablet is
 *       single-owner, F1 — but summing across nodes is harmless and covers
 *       the transient window of a partition move without a special case).
 */
class TCrossNodeEntry {
public:
    void ApplyDelta(ui32 nodeId, const NKikimrSysView::TDbTabletCounters& diff) {
        TabletType = diff.GetType();
        AggregateIncrementalTabletCounters(&Accumulator, diff);
        PerNodeSnapshot[nodeId] = diff;
    }

    void DropNode(ui32 nodeId) {
        PerNodeSnapshot.erase(nodeId);
    }

    bool IsEmpty() const {
        return PerNodeSnapshot.empty();
    }

    TTabletTypes::EType GetTabletType() const {
        return TabletType;
    }

    TVector<ui32> GetNodeIds() const {
        TVector<ui32> nodeIds;
        nodeIds.reserve(PerNodeSnapshot.size());
        for (const auto& [nodeId, _] : PerNodeSnapshot) {
            nodeIds.push_back(nodeId);
        }
        return nodeIds;
    }

    /**
     * Resum Simple/MAX across every currently live node (DropNode() must
     * already have run for this tick) and publish the result into bucket's
     * counter group.
     */
    void Recalculate(TCountersBucket& bucket, const TTabletCountersBase* executorCountersTemplate) {
        ResetStatefulTabletCounters(&Accumulator);
        for (const auto& [_, snapshot] : PerNodeSnapshot) {
            AggregateStatefulTabletCounters(&Accumulator, snapshot);
        }
        Accumulator.SetType(TabletType);

        bucket.FromProto(Accumulator, executorCountersTemplate);
    }

private:
    TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;
    NKikimrSysView::TDbTabletCounters Accumulator;
    THashMap<ui32, NKikimrSysView::TDbTabletCounters> PerNodeSnapshot;
};

/**
 * A single (tablet_id, follower_id) leaf of a PARTITION-level table.
 */
struct TLeafEntry {
    TCrossNodeEntry Cross;

    THolder<TCountersBucket> RawBucket;
    NMonitoring::TDynamicCounterPtr PublicLeafGroup;
    TYdbMetricsMapperPtr Mapper;
};

/**
 * Everything the processor keeps for a single table, keyed by its path
 * relative to the database (the very same keying choice, and the very same
 * reason, as the node builder: two TPathIds sharing one path collapse into
 * one entry instead of aliasing one counter group).
 *
 * @note Only one of the two shapes is ever populated at a time, chosen by
 *       Level — enforced by GetOrCreateTableEntry() tearing the whole entry
 *       down and rebuilding it fresh the moment Level disagrees with what a
 *       message says now (a table's METRICS_LEVEL changed, or ITS OWN
 *       identity moved to another path and a new one arrived at this one).
 */
struct TTableEntry {
    EMetricsLevel Level = TMetricsSettings::MetricsLevelUnspecified;

    NMonitoring::TDynamicCounterPtr RawTableGroup;    // table=<path>, in the raw tree
    NMonitoring::TDynamicCounterPtr PublicTableGroup; // table=<path>, in the public tree

    // TABLE level
    THolder<TCrossNodeEntry> TableCross;
    THolder<TCountersBucket> TableRawBucket;
    TYdbMetricsMapperPtr TableMapper;

    // PARTITION level
    THashMap<TTabletKey, THolder<TLeafEntry>> Leaves;
    TYdbMetricsAggregatorPtr TableAggregator; // sums the leaves' public groups into PublicTableGroup
};

TString LeafSourceId(const TTabletKey& tabletKey) {
    return TStringBuilder() << tabletKey.first << ":" << tabletKey.second;
}

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
        , DatabasePrefix(ChopTrailingSlash(databasePath))
        , ExecutorCountersTemplate(std::move(executorCountersTemplate))
    {}

    void ApplyFromNode(
        ui32 nodeId,
        bool isFollowerRole,
        const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables
    ) override {
        THashSet<TString> newTableContributions;
        THashSet<TTabletKey> newLeafContributions;

        for (const auto& table : tables) {
            const TString relativePath = TString(MakeRelativeTablePath(DatabasePrefix, table.GetTablePath()));
            const EMetricsLevel level = table.GetLevel();

            if (level == TMetricsSettings::MetricsLevelTable) {
                if (isFollowerRole) {
                    // S1'': the follower Tablet Counters Aggregator builds no TABLE
                    // collapse bucket, so this can only mean the wire (or the caller
                    // choosing which role's aggregator it packed) is confused about
                    // roles. Drop rather than sum it — summing would silently double
                    // the leader's own contribution into a "cross-node" accumulator
                    // that is supposed to be leader-sourced by construction.
                    //
                    // Dropped silently rather than asserted on: this is remote input
                    // off the wire, so a node running older or broken code must not
                    // be able to abort the SysView Processor's debug build. The
                    // Y_DEBUG_ABORT_UNLESS assertions elsewhere in the detailed
                    // metrics code all guard LOCAL invariants instead.
                    continue;
                }

                ApplyTableLevel(relativePath, nodeId, table.GetTableCounters());
                newTableContributions.insert(relativePath);
            } else if (level == TMetricsSettings::MetricsLevelPartition) {
                auto& entry = GetOrCreateTableEntry(relativePath, level);

                for (const auto& leafProto : table.GetLeaves()) {
                    const TTabletKey tabletKey(leafProto.GetTabletId(), leafProto.GetFollowerId());

                    EvictMovedLeaf(tabletKey, relativePath);

                    const TTabletTypes::EType tabletType = leafProto.GetCounters().GetType();
                    auto* leaf = GetOrCreateLeaf(entry, relativePath, tabletKey, tabletType);
                    if (!leaf) {
                        // An unpublished tablet type (GetDetailedMetricsCounterNames
                        // returned nullptr): matches the node's own AddCounters, which
                        // never lets such a tablet reach Pack() in the first place, so
                        // this is defensive rather than an expected path.
                        continue;
                    }

                    leaf->Cross.ApplyDelta(nodeId, leafProto.GetCounters());
                    newLeafContributions.insert(tabletKey);
                }
            }
            // MetricsLevelUnspecified/Disabled: the node's Pack() never emits an
            // entry for a table it holds neither a bucket nor a leaf for, so there
            // is nothing to apply here.
        }

        ReconcileStream(TStreamKey(nodeId, isFollowerRole), std::move(newTableContributions), std::move(newLeafContributions));
    }

    void DropNode(ui32 nodeId) override {
        ReconcileStream(TStreamKey(nodeId, false), {}, {});
        ReconcileStream(TStreamKey(nodeId, true), {}, {});
    }

    void RecalculateAllCounters() override {
        for (auto& [path, entry] : Tables) {
            if (entry.TableCross && entry.TableRawBucket) {
                entry.TableCross->Recalculate(*entry.TableRawBucket, ExecutorCountersTemplate.Get());
                entry.TableMapper->TransferCounterValues();
            }

            for (auto& [tabletKey, leaf] : entry.Leaves) {
                leaf->Cross.Recalculate(*leaf->RawBucket, ExecutorCountersTemplate.Get());
                leaf->Mapper->TransferCounterValues();
            }

            if (entry.TableAggregator) {
                entry.TableAggregator->RecalculateAllTargetCounters();
            }
        }
    }

private:
    using TStreamKey = std::pair<ui32, bool>;

    /**
     * @return The existing entry for relativePath, rebuilt from scratch if
     *         its stored Level disagrees with level (a METRICS_LEVEL change,
     *         or this path being reused by a different table altogether) —
     *         a fresh entry otherwise.
     */
    TTableEntry& GetOrCreateTableEntry(const TString& relativePath, EMetricsLevel level) {
        auto it = Tables.find(relativePath);
        if (it != Tables.end()) {
            if (it->second.Level == level) {
                return it->second;
            }
            DropTableEntryCompletely(it);
        }

        auto& entry = Tables[relativePath];
        entry.Level = level;
        entry.RawTableGroup = RawCounterGroup->GetSubgroup(TABLE_LABEL, relativePath);
        entry.PublicTableGroup = TargetCounterGroup->GetSubgroup(TABLE_LABEL, relativePath);
        return entry;
    }

    void ApplyTableLevel(const TString& relativePath, ui32 nodeId, const NKikimrSysView::TDbTabletCounters& diff) {
        auto& entry = GetOrCreateTableEntry(relativePath, TMetricsSettings::MetricsLevelTable);

        if (!entry.TableCross) {
            entry.TableCross = MakeHolder<TCrossNodeEntry>();
        }
        entry.TableCross->ApplyDelta(nodeId, diff);

        if (!entry.TableRawBucket) {
            const TTabletTypes::EType tabletType = entry.TableCross->GetTabletType();
            const TDetailedMetricsCounterNames* counterNames = GetDetailedMetricsCounterNames(tabletType);
            if (counterNames) {
                entry.TableRawBucket = MakeHolder<TCountersBucket>(
                    entry.RawTableGroup, tabletType, *counterNames, RawCounterGroup->Visibility()
                );
                entry.TableMapper = CreateYdbMetricsMapperByTabletType(
                    tabletType, entry.PublicTableGroup, entry.RawTableGroup
                );
            }
        }
    }

    /**
     * @return The leaf's state, creating it (and its raw/public groups and
     *         its source registration in the table's TYdbMetricsAggregator)
     *         on first sight — or nullptr if tabletType publishes no
     *         detailed metrics at all (GetDetailedMetricsCounterNames)
     */
    TLeafEntry* GetOrCreateLeaf(
        TTableEntry& tableEntry,
        const TString& relativePath,
        const TTabletKey& tabletKey,
        TTabletTypes::EType tabletType
    ) {
        auto it = tableEntry.Leaves.find(tabletKey);
        if (it != tableEntry.Leaves.end()) {
            return it->second.Get();
        }

        const TDetailedMetricsCounterNames* counterNames = GetDetailedMetricsCounterNames(tabletType);
        if (!counterNames) {
            return nullptr;
        }

        NMonitoring::TDynamicCounterPtr rawLeafGroup = tableEntry.RawTableGroup
            ->GetSubgroup(DETAILED_METRICS_LABEL, PER_PARTITION_VALUE)
            ->GetSubgroup(TABLET_ID_LABEL, ToString(tabletKey.first))
            ->GetSubgroup(FOLLOWER_ID_LABEL, ToString(tabletKey.second));

        NMonitoring::TDynamicCounterPtr publicLeafGroup = tableEntry.PublicTableGroup
            ->GetSubgroup(TABLET_ID_LABEL, ToString(tabletKey.first))
            ->GetSubgroup(FOLLOWER_ID_LABEL, ToString(tabletKey.second));

        auto holder = MakeHolder<TLeafEntry>();
        holder->RawBucket = MakeHolder<TCountersBucket>(
            rawLeafGroup, tabletType, *counterNames, RawCounterGroup->Visibility()
        );
        holder->PublicLeafGroup = publicLeafGroup;
        holder->Mapper = CreateYdbMetricsMapperByTabletType(tabletType, publicLeafGroup, rawLeafGroup);

        if (!tableEntry.TableAggregator) {
            tableEntry.TableAggregator = CreateYdbMetricsAggregatorByTabletType(tabletType, tableEntry.PublicTableGroup);
        }
        // step 09.5: a follower leaf leaves every LeaderOnly metric's slot empty,
        // so table.datashard.row_count and friends are not inflated by summing
        // every replica's copy of the very same absolute value/accumulated delta
        tableEntry.TableAggregator->AddSourceCountersGroup(
            LeafSourceId(tabletKey), publicLeafGroup, tabletKey.second != 0 /* isFollowerSource */
        );

        LeafToTableMap[tabletKey] = relativePath;

        auto* leaf = holder.Get();
        tableEntry.Leaves[tabletKey] = std::move(holder);
        return leaf;
    }

    /**
     * If tabletKey is currently attributed to a DIFFERENT table than
     * relativePath (a partition moved to another table under a schema
     * change, or its table was renamed), evict it from there completely —
     * every remaining contributor, not just the one this call is about to
     * re-add under the new table — so the leaf restarts cleanly under
     * relativePath instead of aliasing state across two tables.
     */
    void EvictMovedLeaf(const TTabletKey& tabletKey, const TString& relativePath) {
        auto mapIt = LeafToTableMap.find(tabletKey);
        if (mapIt == LeafToTableMap.end() || mapIt->second == relativePath) {
            return;
        }

        auto tableIt = Tables.find(mapIt->second);
        if (tableIt == Tables.end()) {
            LeafToTableMap.erase(mapIt);
            return;
        }

        auto leafIt = tableIt->second.Leaves.find(tabletKey);
        if (leafIt == tableIt->second.Leaves.end()) {
            LeafToTableMap.erase(mapIt);
            return;
        }

        // Snapshot the node IDs up front: RemoveLeafContribution() erases the leaf
        // (and possibly the whole old table entry) once the LAST one is dropped
        for (ui32 nodeId : leafIt->second->Cross.GetNodeIds()) {
            RemoveLeafContribution(tabletKey, nodeId);
        }
    }

    /**
     * Reconcile one (nodeId, isFollowerRole) stream's report against what it
     * reported last time: anything it used to contribute to but does not
     * mention now loses this nodeId from that entry's contributor set.
     */
    void ReconcileStream(TStreamKey stream, THashSet<TString> newTables, THashSet<TTabletKey> newLeaves) {
        auto tableIt = StreamTableContributions.find(stream);
        if (tableIt != StreamTableContributions.end()) {
            for (const auto& path : tableIt->second) {
                if (!newTables.contains(path)) {
                    RemoveTableContribution(path, stream.first);
                }
            }
        }
        if (newTables.empty()) {
            if (tableIt != StreamTableContributions.end()) {
                StreamTableContributions.erase(tableIt);
            }
        } else {
            StreamTableContributions[stream] = std::move(newTables);
        }

        auto leafIt = StreamLeafContributions.find(stream);
        if (leafIt != StreamLeafContributions.end()) {
            for (const auto& tabletKey : leafIt->second) {
                if (!newLeaves.contains(tabletKey)) {
                    RemoveLeafContribution(tabletKey, stream.first);
                }
            }
        }
        if (newLeaves.empty()) {
            if (leafIt != StreamLeafContributions.end()) {
                StreamLeafContributions.erase(leafIt);
            }
        } else {
            StreamLeafContributions[stream] = std::move(newLeaves);
        }
    }

    void RemoveTableContribution(const TString& relativePath, ui32 nodeId) {
        auto it = Tables.find(relativePath);
        if (it == Tables.end() || !it->second.TableCross) {
            return;
        }

        it->second.TableCross->DropNode(nodeId);
        if (it->second.TableCross->IsEmpty()) {
            DropTableEntryCompletely(it);
        }
    }

    void RemoveLeafContribution(const TTabletKey& tabletKey, ui32 nodeId) {
        auto mapIt = LeafToTableMap.find(tabletKey);
        if (mapIt == LeafToTableMap.end()) {
            return;
        }

        auto tableIt = Tables.find(mapIt->second);
        if (tableIt == Tables.end()) {
            LeafToTableMap.erase(mapIt);
            return;
        }

        auto leafIt = tableIt->second.Leaves.find(tabletKey);
        if (leafIt == tableIt->second.Leaves.end()) {
            LeafToTableMap.erase(mapIt);
            return;
        }

        leafIt->second->Cross.DropNode(nodeId);
        if (!leafIt->second->Cross.IsEmpty()) {
            return;
        }

        if (tableIt->second.TableAggregator) {
            tableIt->second.TableAggregator->RemoveSourceCountersGroup(LeafSourceId(tabletKey));
        }

        RawCounterGroup->RemoveSubgroupChain({
            {TABLE_LABEL, tableIt->first},
            {DETAILED_METRICS_LABEL, PER_PARTITION_VALUE},
            {TABLET_ID_LABEL, ToString(tabletKey.first)},
            {FOLLOWER_ID_LABEL, ToString(tabletKey.second)},
        });
        TargetCounterGroup->RemoveSubgroupChain({
            {TABLE_LABEL, tableIt->first},
            {TABLET_ID_LABEL, ToString(tabletKey.first)},
            {FOLLOWER_ID_LABEL, ToString(tabletKey.second)},
        });

        tableIt->second.Leaves.erase(leafIt);
        LeafToTableMap.erase(mapIt);

        if (tableIt->second.Leaves.empty()) {
            DropTableEntryCompletely(tableIt);
        }
    }

    /**
     * Tear the whole entry down regardless of shape (TABLE bucket or
     * PARTITION leaves) and regardless of whether it is actually empty:
     * used both for a clean empty-teardown and for GetOrCreateTableEntry's
     * Level-changed rebuild.
     */
    void DropTableEntryCompletely(THashMap<TString, TTableEntry>::iterator it) {
        auto& entry = it->second;

        entry.TableMapper.Reset();
        entry.TableRawBucket.Reset();
        entry.TableCross.Reset();
        entry.TableAggregator.Reset();

        for (const auto& [tabletKey, _] : entry.Leaves) {
            LeafToTableMap.erase(tabletKey);
        }
        entry.Leaves.clear();

        RawCounterGroup->RemoveSubgroupChain({{TABLE_LABEL, it->first}});
        TargetCounterGroup->RemoveSubgroupChain({{TABLE_LABEL, it->first}});

        Tables.erase(it);
    }

private:
    NMonitoring::TDynamicCounterPtr RawCounterGroup;
    NMonitoring::TDynamicCounterPtr TargetCounterGroup;

    const TString DatabasePrefix;

    THolder<TTabletCountersBase> ExecutorCountersTemplate;

    THashMap<TString, TTableEntry> Tables;

    /**
     * Reverse map from (tabletId, followerId) to the table's relative path,
     * used by EvictMovedLeaf() and by RemoveLeafContribution() (the leaf's
     * own identity carries no table path once it is stored, exactly the
     * TabletToTableMap role plays on the node).
     */
    THashMap<TTabletKey, TString> LeafToTableMap;

    /**
     * What each (nodeId, isFollowerRole) stream contributed as of its last
     * ApplyFromNode() call, for absence detection (see the class doc
     * comment on TStreamKey above).
     */
    THashMap<TStreamKey, THashSet<TString>> StreamTableContributions;
    THashMap<TStreamKey, THashSet<TTabletKey>> StreamLeafContributions;
};

} // namespace

TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr rawCounterGroup,
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    THolder<TTabletCountersBase> executorCountersTemplate
) {
    return MakeIntrusive<TProcessorDatabaseMetricsAggregatorImpl>(
        rawCounterGroup,
        targetCounterGroup,
        databasePath,
        std::move(executorCountersTemplate)
    );
}

} // namespace NKikimr
