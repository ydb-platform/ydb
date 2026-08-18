#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {

struct TDetailedMetricsCounterNames;

/**
 * Labels of the detailed metrics counter tree, shared by the node builder
 * (node_database_metrics_aggregator.{h,cpp}) and the processor builder
 * (processor_database_metrics_aggregator.{h,cpp}, step 09): both grow the
 * very same shape (S4) below their own root — the node's ydb_detailed_raw,
 * the processor's private raw group and public ydb_detailed group — just
 * fed from a different source (a live TTabletCountersBase on the node,
 * NKikimrSysView::TDbTabletCounters off the wire on the processor).
 */
inline const TString DATABASE_LABEL = "database";
inline const TString TABLE_LABEL = "table";
inline const TString DETAILED_METRICS_LABEL = "detailed_metrics";
inline const TString TABLET_ID_LABEL = "tablet_id";
inline const TString FOLLOWER_ID_LABEL = "follower_id";

inline const TString PER_PARTITION_VALUE = "per_partition";

// Labels of the low level tablet counters (the same as in the "tablets" group)
inline const TString TYPE_LABEL = "type";
inline const TString CATEGORY_LABEL = "category";

inline const TString EXECUTOR_CATEGORY = "executor";
inline const TString APP_CATEGORY = "app";

/**
 * A single tablet (a leader or a follower) within a table.
 */
using TTabletKey = std::pair<ui64, ui32>;

/**
 * @return path with any trailing "/" chopped, as a view into path.
 */
TStringBuf ChopTrailingSlash(const TStringBuf path);

/**
 * Strip the database path prefix from the full path of the table.
 *
 * @param[in] databasePrefix The database path with the trailing "/" already chopped
 *                            (see ChopTrailingSlash)
 * @param[in] tablePath The full path of the table, which outlives the returned view
 *
 * @return A view into tablePath: either the stripped suffix, or tablePath itself
 *         when it does not start with the database. No allocation either way.
 */
TStringBuf MakeRelativeTablePath(const TStringBuf databasePrefix, const TString& tablePath);

/**
 * A single bucket of the detailed metrics counter tree: the low level counters
 * of one or more tablets of the same type.
 *
 * @note The bucket of a table level table holds many tablets, while a leaf group
 *       of a partition level table holds exactly one. Both cases are handled by
 *       the very same code: aggregating a single tablet is a passthrough
 *       (SUM(x) == MAX(x) == x).
 *
 * @note Two callers fill this very same bucket two different ways (step 09).
 *       The node (Apply()/Forget(), one call per live tablet) sums many
 *       TABLETS into it, off a live TTabletCountersBase it is handed, and
 *       Initialize()s the aggregate's layout lazily off that very same
 *       object on the first Apply(). The processor (FromProto(), one call
 *       per RecalculateAllCounters() tick) instead sums many NODES into it
 *       one level up (TCrossNodeEntry, processor_database_metrics_
 *       aggregator.cpp) and hands this bucket the already-summed
 *       NKikimrSysView::TDbTabletCounters directly — it never sees a live
 *       TTabletCountersBase, so EnsureInitialized() stands in for the
 *       Initialize()-off-the-first-Apply() path, initializing off the very
 *       same template counter sets the node built its own layout from
 *       (CreateAppCountersByTabletType() for the app category; the executor
 *       category's template is handed in by the caller, ydb/core/tablet
 *       cannot include ydb/core/tablet_flat/flat_executor_counters.h itself
 *       — see processor_database_metrics_aggregator.h's factory comment).
 */
class TCountersBucket {
public:
    TCountersBucket(
        NMonitoring::TDynamicCounterPtr bucketGroup,
        TTabletTypes::EType tabletType,
        const TDetailedMetricsCounterNames& counterNames,
        NMonitoring::TCountableBase::EVisibility visibility
    );

    void Apply(
        const TTabletKey& tablet,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now
    );

    void Forget(const TTabletKey& tablet);

    bool IsEmpty() const;

    void RecalcAll();

    /**
     * Fill out with this bucket's contribution for the given generation: Simple
     * absolute stateful, Cumulative/HIST delta since the previous call for the
     * SAME generation (see TNodeDatabaseMetricsAggregator::Pack's doc comment
     * for the full contract).
     */
    void Pack(NKikimrSysView::TDbTabletCounters& out, ui64 generation);

    /**
     * Initialize both aggregates from template counter sets rather than off a
     * live tablet's own counters (the node's path, Apply()). A no-op once
     * already initialized, by either this or Apply().
     *
     * @param[in] tabletType Must match the type this bucket was constructed with.
     * @param[in] executorCountersTemplate The template to Initialize() the
     *            executor category from. Owned by the caller, only borrowed here
     *            (a single template instance is reusable across every bucket:
     *            the executor category's layout does not vary by tablet type).
     */
    void EnsureInitialized(
        TTabletTypes::EType tabletType,
        const TTabletCountersBase* executorCountersTemplate
    );

    /**
     * Fill this bucket's counter group from an already cross-node-summed
     * NKikimrSysView::TDbTabletCounters (the processor's path), bypassing
     * Apply()'s per-tablet SUM/MAX bookkeeping: the caller already did that
     * summation itself, across nodes rather than across tablets (see
     * TCrossNodeEntry, processor_database_metrics_aggregator.cpp). Calls
     * EnsureInitialized() first.
     */
    void FromProto(
        NKikimrSysView::TDbTabletCounters& proto,
        const TTabletCountersBase* executorCountersTemplate
    );

private:
    TTabletTypes::EType TabletType;

    NMonitoring::TDynamicCounterPtr TypeGroup;

    NPrivate::TAggregatedTabletCounters ExecutorCounters;
    NPrivate::TAggregatedTabletCounters AppCounters;

    const TDetailedMetricsCounterNames* CounterNames;

    THashMap<TTabletKey, ui64> SourceIds;
    ui64 NextSourceId = 0;

    /**
     * The pack baseline (step 08): Current is this bucket's absolute snapshot
     * as of LastPackedGeneration, Confirmed is the snapshot Current replaced,
     * used as the Cumulative/HIST diff baseline. Both start empty, which is
     * exactly what an unstarted diff baseline should be: the very first Pack()
     * reports every non-zero Cumulative/HIST value as its own full delta.
     */
    NKikimrSysView::TDbTabletCounters Current;
    NKikimrSysView::TDbTabletCounters Confirmed;
    ui64 LastPackedGeneration = 0;
    bool HasPacked = false;
};

} // namespace NKikimr
