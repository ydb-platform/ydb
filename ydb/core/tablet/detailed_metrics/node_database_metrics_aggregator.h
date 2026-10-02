#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NKikimr {

/**
 * Guards the VALUES published into the detailed metrics counter tree, so that a reader
 * never observes an aggregate midway through being republished.
 *
 * A reader MUST hold it across its whole traversal. Locking from inside a traversal
 * deadlocks.
 */
TMutex& DetailedMetricsLock();

/**
 * The per-table detailed metrics settings, as stored in the schema.
 */
using TDetailedMetricsSettings = NKikimrSchemeOp::TTableDetailedMetricsSettings;

/**
 * The effective level at which detailed metrics are collected for a single table.
 */
using EDetailedMetricsLevel = TDetailedMetricsSettings::EMetricsLevel;

/**
 * The identity of the user table, whose tablet reports the low level counters.
 */
struct TDetailedMetricsTableInfo {
    TPathId TableId;

    /**
     * The full path of the table, for example, /Root/db/dir/table. The database path
     * prefix is stripped before the path is used as the value of the "table" label.
     */
    TString TablePath;

    ui64 SchemaVersion = 0;

    EDetailedMetricsLevel MetricsLevel = TDetailedMetricsSettings::MetricsLevelUnspecified;
};

/**
 * The per-node, per-database, per-role aggregator of the detailed metrics.
 *
 * The instance keeps the public metric values (see TDetailedMetricsDescriptor) of every bucket
 * of its role, which Pack() reports to the SysView Processor:
 * - Table level: a TABLE bucket per table, all the same-node leaders of the table collapsed
 *   (the followers of a table level table are not collected on the node);
 * - Partition level: a PARTITION leaf per tablet of either role, which is kept as its public
 *   metric values only (a few hundred bytes) and owns no counter group.
 *
 * The TABLE buckets also publish their low level counter aggregates, a debug view refreshed
 * by RecalculateAllCounters() only, in the counter group the instance is handed. Both instances
 * of a node are handed the very same group, which only the instance of the leaders fills:
 *
 *     ydb_detailed_raw                        (private, created by the caller)
 *       |
 *       +-- the target group of BOTH instances
 *           database=<database path>
 *             table=<table path relative to the database>
 *               type=<tablet type>
 *                 category=executor|app       the collapsed counters of the table (leaders only)
 *
 * The type=/category= subtree is the very same layout as the node wide "tablets" group.
 * The groups of a table exist only while its TABLE bucket does, so a partition level table
 * creates no counter group at all.
 */
class TNodeDatabaseMetricsAggregator : public NSysView::IDbDetailedCounters {
public:
    /**
     * @param[in] now Used to differentiate the cumulative counters into per second rates
     *
     * @note The public metrics of the tablet type (see TDetailedMetricsDescriptor) define what
     *       every bucket reports, so the cardinality does not depend on the counter sets. Only
     *       a TABLE bucket publishes series of its own: an allow-listed set of low level counters
     *       in the counter tree
     */
    virtual void AddCounters(
        const TString& tablePath,
        EDetailedMetricsLevel metricsLevel,
        ui64 tabletId,
        ui32 followerId,
        TTabletTypes::EType tabletType,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now
    ) = 0;

    /**
     * Drop everything this tablet contributed, removing the counter groups, which are
     * left empty.
     *
     * @param[in] tabletId The tablet ID and its role are sufficient: this class owns
     *                      the reverse map from (tabletId, followerId) -> the table's
     *                      relative path (the same key the table entries and their
     *                      counter groups are addressed by), because the forget event
     *                      from the Tablet Counters Aggregator carries no table identity.
     *
     * @note A tablet of an unknown table is silently ignored, and forgetting a tablet
     *       twice is not an error.
     *
     * @note A tablet reports exactly one table, so the reverse map holds one table per
     *       tablet. A tablet, which is re-reported under another table, is moved: its
     *       contribution to the previous table is dropped by AddCounters, because
     *       nothing but the reverse map could reach it afterwards.
     */
    virtual void ForgetTablet(ui64 tabletId, ui32 followerId) = 0;

    virtual void RecalculateAllCounters() = 0;
};

using TNodeDatabaseMetricsAggregatorPtr = TIntrusivePtr<TNodeDatabaseMetricsAggregator>;

/**
 * @param[in] targetCounterGroup The group to fill with the low level counters of the TABLE buckets
 * @param[in] isFollowerRole The role of the tablets this instance is fed
 */
TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    bool isFollowerRole
);

} // namespace NKikimr
