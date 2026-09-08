#include "processor_database_metrics_aggregator.h"

#include "detailed_metrics_counter_set.h"
#include "detailed_metrics_tree.h"
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

// BucketsCount and CumulativeCount are uint64 straight off the wire
// (NKikimrSysView::TDbCounters, ydb/core/protos/sys_view.proto), so an
// unbounded Resize() driven by either is an allocation a hostile or
// corrupted peer fully controls. A real counter set tops out at a few
// hundred entries, so a few thousand is already far above anything
// legitimate.
constexpr size_t MAX_WIRE_COUNTER_COUNT = 4096;

template <bool IsMax>
struct TAggregateCumulative {
    static void Apply(NKikimrSysView::TDbCounters* dst, const NKikimrSysView::TDbCounters& src) {
        size_t cumulativeSize = src.GetCumulativeCount();
        auto histogramSize = src.HistogramSize();

        if (cumulativeSize > MAX_WIRE_COUNTER_COUNT) {
            // Verbatim clone of ydb/core/sys_view/processor/db_counters.cpp:53-56,
            // which has the same defect and should get the same fix separately.
            return;
        }

        if ((size_t)dst->CumulativeSize() < cumulativeSize) {
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
            if ((size_t)index >= cumulativeSize) {
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
            size_t bucketCount = histogram.GetBucketsCount();
            if (bucketCount > MAX_WIRE_COUNTER_COUNT) {
                // Same wire-controlled-allocation defect as cumulativeSize above,
                // but per histogram: skip only this histogram, not the whole entry.
                continue;
            }
            if ((size_t)to->size() < bucketCount) {
                to->Resize(bucketCount, 0);
            }
            auto doubleDiffSize = from.size();
            for (int b = 0; b < doubleDiffSize; ) {
                auto index = from[b++];
                auto value = from[b++];
                if ((size_t)index >= bucketCount) {
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
 * Fold one node's latest Simple snapshot, and MAX (both its Simple and its
 * Cumulative per-second-rate parts), into the cross-node accumulator
 * (AggregateStatefulCounters convention, S2).
 *
 * @param[in] singleOwner Whether the bucket this snapshot belongs to has
 *            exactly one legitimate contributing node at a time (a
 *            PARTITION leaf, F1: the tablet is single-owner, two nodes
 *            reporting it at once is only ever the transient of a partition
 *            move) as opposed to many (a TABLE collapse bucket, S1'', with
 *            disjoint per-node contributions). Simple/gauge values are
 *            summed across nodes for the latter but MAX'd for the former —
 *            summing two copies of the SAME leaf's row_count/size_bytes
 *            during a move would double it. Cumulative/HIST MAX needs no
 *            such branch: it is already a per-node latest-rate snapshot, and
 *            taking the max of two copies of the same value is a no-op.
 */
void AggregateStatefulTabletCounters(
    NKikimrSysView::TDbTabletCounters* dst,
    const NKikimrSysView::TDbTabletCounters& src,
    bool singleOwner
) {
    if (singleOwner) {
        TAggregateSimple<true>::Apply(dst->MutableExecutorCounters(), src.GetExecutorCounters());
        TAggregateSimple<true>::Apply(dst->MutableAppCounters(), src.GetAppCounters());
    } else {
        TAggregateSimple<false>::Apply(dst->MutableExecutorCounters(), src.GetExecutorCounters());
        TAggregateSimple<false>::Apply(dst->MutableAppCounters(), src.GetAppCounters());
    }

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
 * @note Cumulative/HIST is always summed across nodes, TABLE bucket or
 *       PARTITION leaf alike: those are disjoint per-node deltas either way
 *       (many leader-sourced tables for a TABLE bucket, S1''; at most one
 *       node's worth of real delta plus a departing owner's tail for a
 *       PARTITION leaf), so adding them is correct in both cases.
 *
 *       Simple/gauges are NOT: correct to sum across the many disjoint
 *       nodes of a TABLE collapse bucket, but a PARTITION leaf's tablet is
 *       single-owner (F1) — seeing it from two nodes at once is only ever
 *       the transient of a partition move (old and new owner both still
 *       reporting the same (tablet, follower) for up to one 5 s tick), and
 *       summing would double row_count/size_bytes for that tick instead of
 *       reporting either owner's real value. SingleOwner selects MAX
 *       instead of SUM for exactly that case (AggregateStatefulTabletCounters).
 */
class TCrossNodeEntry {
public:
    explicit TCrossNodeEntry(bool singleOwner)
        : SingleOwner(singleOwner)
    {}

    /**
     * @return false, with nothing applied and nothing recorded, if
     *         TabletType is already fixed (a previous ApplyDelta already set
     *         it) and diff.GetType() disagrees — dropped rather than
     *         asserted on, since this is remote input off the wire, same
     *         reasoning as the follower-TABLE-partial comment in
     *         ApplyFromNode. Mirrors the node side's explicit drift check
     *         (node_database_metrics_aggregator.cpp:116-131): without it, a
     *         mismatched layout would be silently misattributed by position
     *         instead of caught, because TCountersBucket::FromProto always
     *         hands EnsureInitialized() the bucket's own construction-time
     *         type, so its Y_DEBUG_ABORT_UNLESS can never fire on this.
     *         true otherwise.
     */
    bool ApplyDelta(ui32 nodeId, const NKikimrSysView::TDbTabletCounters& diff) {
        if (TabletType != TTabletTypes::TypeInvalid && diff.GetType() != TabletType) {
            return false;
        }
        TabletType = diff.GetType();
        AggregateIncrementalTabletCounters(&Accumulator, diff);
        PerNodeSnapshot[nodeId] = diff;
        return true;
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
            AggregateStatefulTabletCounters(&Accumulator, snapshot, SingleOwner);
        }
        Accumulator.SetType(TabletType);

        bucket.FromProto(Accumulator, executorCountersTemplate);
    }

private:
    const bool SingleOwner;
    TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;
    NKikimrSysView::TDbTabletCounters Accumulator;
    THashMap<ui32, NKikimrSysView::TDbTabletCounters> PerNodeSnapshot;
};

/**
 * A single (tablet_id, follower_id) leaf of a PARTITION-level table.
 */
struct TLeafEntry {
    TCrossNodeEntry Cross{true}; // single-owner: the tablet behind a leaf, F1

    THolder<TCountersBucket> RawBucket;
    TYdbMetricsMapperPtr Mapper;
};

/**
 * Everything the processor keeps for a single table, keyed by its path
 * relative to the database (the very same keying choice, and the very same
 * reason, as the node builder: two TPathIds sharing one path collapse into
 * one entry instead of aliasing one counter group).
 *
 * @note Only one of the two shapes is ever populated at a time, chosen by
 *       Level. Kept stable, NOT torn down and rebuilt, for as long as an
 *       incoming message's Level agrees with it: ApplyFromNode's LevelMatches()
 *       guard keeps a message whose Level disagrees from ever touching this
 *       entry at all, so it survives untouched until every contributor of
 *       the OLD shape stops mentioning it (ReconcileStream's normal absence
 *       detection) — a METRICS_LEVEL flip reaches nodes asynchronously, and
 *       tearing the shared entry down on the first disagreeing node would
 *       destroy every OTHER still-agreeing node's live contribution with it.
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
    {
        // A local invariant on a constructor argument, not remote input (same
        // precedent as TTabletCountersForDb::FromProto, tablet_counters_
        // aggregator.cpp): TAggregatedTabletCounters::Initialize() guards its
        // whole body on `if (counters)` and still sets IsInitialized = true
        // with FullSize* == 0, so a null template would silently zero every
        // executor metric forever instead of failing loudly here.
        Y_ABORT_UNLESS(ExecutorCountersTemplate, "executorCountersTemplate must not be null");
    }

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

                if (ApplyTableLevel(relativePath, nodeId, table.GetTableCounters())) {
                    newTableContributions.insert(relativePath);
                }
            } else if (level == TMetricsSettings::MetricsLevelPartition) {
                if (!LevelMatches(relativePath, level)) {
                    // A METRICS_LEVEL flip reaches nodes asynchronously: this message
                    // disagrees with the entry's current (TABLE) shape, which some
                    // OTHER, not-yet-converged node may still be legitimately feeding.
                    // Deliberately NOT touching the entry and NOT recording a
                    // contribution here: leaving this table out of
                    // newLeafContributions lets ReconcileStream's normal absence
                    // detection retire the old shape one tick after its last
                    // contributor stops feeding it, and the next message builds the
                    // new (PARTITION) shape cleanly once nothing disagrees anymore.
                    continue;
                }

                // Seeded from any already-existing entry (this table's Level was
                // just confirmed to match, so it is safe to keep using as-is);
                // stays null and is filled lazily inside GetOrCreateLeaf, on the
                // first leaf that actually resolves, when the table is new — so a
                // message whose leaves ALL fail to resolve (empty Leaves, or every
                // GetOrCreateLeaf nullptr) never creates an entry nothing will ever
                // reach again.
                TTableEntry* entry = Tables.FindPtr(relativePath);

                for (const auto& leafProto : table.GetLeaves()) {
                    const TTabletKey tabletKey(leafProto.GetTabletId(), leafProto.GetFollowerId());

                    EvictMovedLeaf(tabletKey, relativePath);

                    const TTabletTypes::EType tabletType = leafProto.GetCounters().GetType();
                    auto* leaf = GetOrCreateLeaf(entry, relativePath, level, tabletKey, tabletType);
                    if (!leaf) {
                        // An unpublished tablet type (GetDetailedMetricsCounterNames
                        // returned nullptr): matches the node's own AddCounters, which
                        // never lets such a tablet reach Pack() in the first place, so
                        // this is defensive rather than an expected path.
                        continue;
                    }

                    if (!leaf->Cross.ApplyDelta(nodeId, leafProto.GetCounters())) {
                        continue;
                    }
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
     * @return Whether relativePath's stored Level (if it has an entry at
     *         all) agrees with level — the guard callers run BEFORE ever
     *         touching an entry, so a message that disagrees never creates,
     *         rebuilds, or otherwise mutates it (see the PARTITION-branch
     *         and ApplyTableLevel comments on why: an in-flight METRICS_LEVEL
     *         change must not let one asynchronously-arriving node tear down
     *         another still-agreeing node's live contribution).
     */
    bool LevelMatches(const TString& relativePath, EMetricsLevel level) const {
        auto it = Tables.find(relativePath);
        return it == Tables.end() || it->second.Level == level;
    }

    /**
     * @return The entry for relativePath, creating (and labeling with
     *         `level`) a fresh one if none exists yet — the existing one
     *         otherwise. Callers are responsible for checking LevelMatches()
     *         first: by the time this runs, level is assumed to already
     *         agree with any existing entry's Level.
     */
    TTableEntry& GetOrCreateTableEntry(const TString& relativePath, EMetricsLevel level) {
        auto it = Tables.find(relativePath);
        if (it != Tables.end()) {
            return it->second;
        }

        auto& entry = Tables[relativePath];
        entry.Level = level;
        entry.RawTableGroup = RawCounterGroup->GetSubgroup(TABLE_LABEL, relativePath);
        entry.PublicTableGroup = TargetCounterGroup->GetSubgroup(TABLE_LABEL, relativePath);
        return entry;
    }

    /**
     * @return Whether the delta was applied and should count as nodeId's
     *         contribution to relativePath's TABLE-level entry for this
     *         tick: false, with nothing applied and nothing recorded, when
     *         relativePath's stored Level disagrees with TABLE (see
     *         LevelMatches) or when TCrossNodeEntry::ApplyDelta rejected the
     *         message for a tablet type mismatch (see its own doc comment).
     */
    bool ApplyTableLevel(const TString& relativePath, ui32 nodeId, const NKikimrSysView::TDbTabletCounters& diff) {
        if (!LevelMatches(relativePath, TMetricsSettings::MetricsLevelTable)) {
            return false;
        }

        auto& entry = GetOrCreateTableEntry(relativePath, TMetricsSettings::MetricsLevelTable);

        if (!entry.TableCross) {
            entry.TableCross = MakeHolder<TCrossNodeEntry>(false); // many disjoint leader-sourced nodes, S1''
        }
        if (!entry.TableCross->ApplyDelta(nodeId, diff)) {
            return false;
        }

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
        return true;
    }

    /**
     * @return The leaf's state, creating it (and its raw/public groups and
     *         its source registration in the table's TYdbMetricsAggregator)
     *         on first sight — or nullptr if tabletType publishes no
     *         detailed metrics at all (GetDetailedMetricsCounterNames).
     *
     * @param[in,out] tableEntry In: nullptr means relativePath has no entry
     *            yet. Out: filled in with relativePath's entry (creating it
     *            via GetOrCreateTableEntry, lazily, only now that tabletType
     *            is known to resolve) whenever this call returns non-null;
     *            left as it was on a nullptr return, so a leaf that fails to
     *            resolve never creates an entry for it. Non-null in is
     *            reused as is (the caller already confirmed relativePath's
     *            Level agrees before ever obtaining a tableEntry to pass).
     */
    TLeafEntry* GetOrCreateLeaf(
        TTableEntry*& tableEntry,
        const TString& relativePath,
        EMetricsLevel level,
        const TTabletKey& tabletKey,
        TTabletTypes::EType tabletType
    ) {
        if (tableEntry) {
            auto it = tableEntry->Leaves.find(tabletKey);
            if (it != tableEntry->Leaves.end()) {
                return it->second.Get();
            }
        }

        const TDetailedMetricsCounterNames* counterNames = GetDetailedMetricsCounterNames(tabletType);
        if (!counterNames) {
            return nullptr;
        }

        if (!tableEntry) {
            tableEntry = &GetOrCreateTableEntry(relativePath, level);
        }

        NMonitoring::TDynamicCounterPtr rawLeafGroup = tableEntry->RawTableGroup
            ->GetSubgroup(DETAILED_METRICS_LABEL, PER_PARTITION_VALUE)
            ->GetSubgroup(TABLET_ID_LABEL, ToString(tabletKey.first))
            ->GetSubgroup(FOLLOWER_ID_LABEL, ToString(tabletKey.second));

        NMonitoring::TDynamicCounterPtr publicLeafGroup = tableEntry->PublicTableGroup
            ->GetSubgroup(TABLET_ID_LABEL, ToString(tabletKey.first))
            ->GetSubgroup(FOLLOWER_ID_LABEL, ToString(tabletKey.second));

        auto holder = MakeHolder<TLeafEntry>();
        holder->RawBucket = MakeHolder<TCountersBucket>(
            rawLeafGroup, tabletType, *counterNames, RawCounterGroup->Visibility()
        );
        holder->Mapper = CreateYdbMetricsMapperByTabletType(tabletType, publicLeafGroup, rawLeafGroup);

        if (!tableEntry->TableAggregator) {
            tableEntry->TableAggregator = CreateYdbMetricsAggregatorByTabletType(tabletType, tableEntry->PublicTableGroup);
        }
        // step 09.5: a follower leaf leaves every LeaderOnly metric's slot empty,
        // so table.datashard.row_count and friends are not inflated by summing
        // every replica's copy of the very same absolute value/accumulated delta
        tableEntry->TableAggregator->AddSourceCountersGroup(
            LeafSourceId(tabletKey), publicLeafGroup, tabletKey.second != 0 /* isFollowerSource */
        );

        LeafToTableMap[tabletKey] = relativePath;

        auto* leaf = holder.Get();
        tableEntry->Leaves[tabletKey] = std::move(holder);
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
     * PARTITION leaves): the empty-teardown case only — RemoveTableContribution
     * calls this once TableCross has lost its last node, RemoveLeafContribution
     * once Leaves has lost its last leaf. (No longer used for a Level-changed
     * rebuild: LevelMatches() now keeps a disagreeing message from ever
     * reaching GetOrCreateTableEntry in the first place, see TTableEntry's
     * class doc.)
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
