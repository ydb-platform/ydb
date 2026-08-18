#pragma once

#include "detailed_metrics_tree.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {

/**
 * The SysView Processor side counterpart of TNodeDatabaseMetricsAggregator
 * (step 09): unions the per-node, per-role TDetailedTableCounters reports
 * of one database (step 08's wire shape) into the public ydb_detailed tree,
 * plus a private raw tree the mapper reads from — the very same two-stage
 * shape S4 chose for the node (raw low level counters, then ONE mapper
 * pass), just fed from the wire instead of from a live tablet, and unioned
 * across NODES instead of across the tablets of one node.
 *
 * Tree shapes built here (S3: table level materialized, replicas_only and
 * the follower_id-dropped partition rollup are NOT — those are one-line
 * consumer queries):
 *
 *     rawCounterGroup                     (private, the mapper's source;
 *      |                                   step 13 attaches database= and
 *      |                                   host="" ABOVE this, the same
 *      |                                   trick TSysViewProcessor::
 *      |                                   AttachExternalCounters uses for
 *      |                                   ydb_serverless — not built here)
 *      +-- table=<relative path>
 *          |
 *          +-- TABLE level: type=<T>/category=<executor|app>   (the cross-
 *          |                node collapse bucket, leader-sourced only, S1'')
 *          |
 *          +-- PARTITION level: detailed_metrics=per_partition
 *                +-- tablet_id=<id>
 *                      +-- follower_id=<n>
 *                            +-- type=<T>/category=<executor|app>
 *
 *     targetCounterGroup                  (public ydb_detailed; step 13
 *      |                                   attaches database= ABOVE this
 *      |                                   the same way)
 *      +-- table=<relative path>
 *            |
 *            +-- <table.datashard.*>       TABLE level: mapped straight from
 *            |                             the raw table= bucket above (it is
 *            |                             leader-sourced by construction).
 *            |                             PARTITION level: the leaf rollup
 *            |                             below, summed by the EXISTING
 *            |                             TYdbMetricsAggregator, leader-only
 *            |                             metrics taken from follower_id==0
 *            |                             leaves alone (step 09.5,
 *            |                             TCounterOptions.LeaderOnly)
 *            |
 *            +-- tablet_id=<id>
 *                  +-- follower_id=<n>
 *                        +-- <table.datashard.*>   PARTITION level leaf, one
 *                                                   TYdbMetricsMapper per leaf
 *
 *      No detailed_metrics= level in the public tree (unlike the raw one):
 *      table/tablet_id/follower_id are the only labels the spec promises
 *      there (exchange/detailed_metrics.txt:311-314).
 *
 * Absence detection (S9/exchange/detailed_metrics.txt:1073-1085): a leader
 * and a follower Tablet Counters Aggregator on the same node send two
 * SEPARATE messages, so "this node stopped reporting table/leaf X" can only
 * be read off the ONE role stream that used to carry X — comparing against
 * the OTHER role's message would evict a TABLE-level table the moment its
 * (structurally table-silent, S1'') follower message arrives. ApplyFromNode
 * therefore tracks contributions per (nodeId, isFollowerRole), not per
 * nodeId alone.
 */
class TProcessorDatabaseMetricsAggregator : public TThrRefBase {
public:
    /**
     * Apply one role stream's WHOLE report from one node for one tick.
     *
     * Encoding, uniform for the TABLE collapse bucket and every PARTITION
     * leaf (S2, the very same convention step 08's Pack() used): Simple/
     * GAUGE absolute stateful (this node's contribution is replaced, not
     * added, every call — the receiver-clears-per-node-state trick that
     * makes a gauge dropping to 0 correct, C2). Cumulative/HIST accumulated
     * as the delta since this node's previous call for the same entry.
     *
     * @param[in] nodeId The reporting node.
     * @param[in] isFollowerRole Which of the node's two Tablet Counters
     *            Aggregator instances packed tables (TABLETS / TABLETS_
     *            FOLLOWERS on the wire, step 11/12). A TABLE-level entry
     *            on the follower stream is rejected — Y_DEBUG_ABORT_UNLESS
     *            plus drop, not sum, in release (S1'': the follower side
     *            builds no TABLE collapse bucket, so this can only mean the
     *            wire or the caller is confused about which role is which).
     * @param[in] tables The WHOLE per-message list, not a delta of the list
     *            itself: a table (or a PARTITION leaf) this stream used to
     *            report and does not mention now loses nodeId from its
     *            contributor set, and once that set empties, the table/leaf
     *            and its groups (raw and public) are dropped.
     */
    virtual void ApplyFromNode(
        ui32 nodeId,
        bool isFollowerRole,
        const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables
    ) = 0;

    /**
     * Drop everything both role streams of this node ever contributed —
     * every leaf, every TABLE-level contribution — evicting the groups this
     * empties. Equivalent to ApplyFromNode(nodeId, false, {}) followed by
     * ApplyFromNode(nodeId, true, {}); spelled out as its own entry point
     * because here it is the NODE that is gone (a lost node, step 13),
     * rather than one particular table it stopped mentioning.
     */
    virtual void DropNode(ui32 nodeId) = 0;

    /**
     * Recompute every published counter from the current cross-node state:
     * resum Simple/MAX across the live contributing nodes of every TABLE
     * bucket and PARTITION leaf, republish into the raw tree, run each
     * bucket's TYdbMetricsMapper (raw -> public), then recompute the
     * PARTITION-level table-to-leaves rollup (TYdbMetricsAggregator).
     *
     * @note Inherits the same non-atomic histogram republish hazard step 07
     *       accepted for the node's own RecalcAll() (TAggregatedTabletCounters
     *       resets a HIST(x) aggregate and refills it bucket by bucket) — see
     *       the step 09 plan's "Inherited hazard" section. Not fixed here.
     */
    virtual void RecalculateAllCounters() = 0;
};

using TProcessorDatabaseMetricsAggregatorPtr = TIntrusivePtr<TProcessorDatabaseMetricsAggregator>;

/**
 * @param[in] rawCounterGroup The private root the raw tree above is built
 *            under (the mapper's source group). Not yet scoped to the
 *            database — step 13 attaches database=/host="" above it.
 * @param[in] targetCounterGroup The public root the ydb_detailed tree above
 *            is built under (the mapper's target group). Likewise not yet
 *            scoped to the database.
 * @param[in] databasePath The database path, used only to make every
 *            table= label relative to it (MakeRelativeTablePath), exactly
 *            like the node builder does.
 * @param[in] executorCountersTemplate The template TTabletCountersBase
 *            EnsureInitialized() derives the executor category's counter
 *            layout from (TAggregatedTabletCounters::Initialize() needs a
 *            live instance of the layout, not just its type). Supplied by
 *            the caller, not constructed in here: ydb/core/tablet cannot
 *            include ydb/core/tablet_flat/flat_executor_counters.h itself
 *            (ydb/core/tablet_flat already PEERDIRs ydb/core/tablet — the
 *            reverse PEERDIR would cycle). This is exactly the constructor-
 *            injection precedent TTabletCountersForDb/CreateTabletDbCounters
 *            already use for the very same reason (tablet_counters_
 *            aggregator.cpp:927,1206, db_counters.cpp:313): the caller
 *            (step 13, living in ydb/core/sys_view/processor, which already
 *            PEERDIRs tablet_flat) constructs `new NTabletFlatExecutor::
 *            TExecutorCounters()` and hands it down type-erased. The app
 *            category needs no such help: CreateAppCountersByTabletType()
 *            already lives in ydb/core/tablet and varies the layout by
 *            tabletType itself, so it is looked up locally, per bucket.
 */
TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr rawCounterGroup,
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    THolder<TTabletCountersBase> executorCountersTemplate
);

} // namespace NKikimr
