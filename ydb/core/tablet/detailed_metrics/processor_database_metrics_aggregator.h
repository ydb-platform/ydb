#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {

/// Union per-node, per-role detailed table counter reports into public and raw trees.
/// Raw tree fed to mapper, public tree is ydb_detailed with database= label above.
/// Shapes: rawCounterGroup.table.detailed_metrics.tablet_id.follower_id.type/category
///         targetCounterGroup.table.tablet_id.follower_id.table.datashard.*
/// Tracks contributions per (nodeId, isFollowerRole) for proper absence detection.
class TProcessorDatabaseMetricsAggregator : public TThrRefBase {
public:
    /// Apply one role stream's whole report from one node. Simple counters are
    /// absolute (receiver-clears per node). Cumulative/HIST are delta since prior call.
    /// Caller must dedup by generation to avoid double-counting.
    /// TABLE-level entries on follower stream are silently dropped (follower has
    /// no TABLE collapse bucket). Absent tables lose that nodeId and are evicted
    /// when all contributors stop mentioning them. TABLE partials and PARTITION
    /// leaves may coexist while the configured metrics level converges.
    virtual void ApplyFromNode(
        ui32 nodeId,
        bool isFollowerRole,
        const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables
    ) = 0;

    /// Drop all contributions from this node (both roles). Evicts empty groups.
    virtual void DropNode(ui32 nodeId) = 0;

    /// Recompute all published counters from current state. Re-derives Simple/MAX
    /// across live nodes, republishes to raw tree, runs mappers, and recalculates
    /// table rollups, including partials and leaves during metrics-level changes.
    virtual void RecalculateAllCounters() = 0;
};

using TProcessorDatabaseMetricsAggregatorPtr = TIntrusivePtr<TProcessorDatabaseMetricsAggregator>;

/// Raw/target counter groups are not yet scoped to database (caller attaches
/// database=/host="" above). Table= labels are made relative to databasePath.
/// executorCountersTemplate: caller-supplied TTabletCountersBase for executor
/// layout initialization (needed because ydb/core/tablet cannot include
/// ydb/core/tablet_flat/flat_executor_counters.h due to circular dependencies).
TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr rawCounterGroup,
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    THolder<TTabletCountersBase> executorCountersTemplate
);

} // namespace NKikimr
