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
 * The per-node, per-database, per-role aggregator of the detailed metrics: Pack() reports
 * the public metric values of every TABLE bucket (the leaders of a table level table collapsed)
 * and every PARTITION leaf (one tablet of either role).
 *
 * The TABLE buckets also fill the target counter group with a debug view of their low level
 * counters, refreshed by RecalculateAllCounters():
 *
 *     ydb_detailed_raw                        (private, created by the caller)
 *       |
 *       +-- the target group of BOTH instances
 *           database=<database path>
 *             table=<table path relative to the database>
 *               type=<tablet type>/category=executor|app
 *
 * The groups live as long as the TABLE buckets under them, so a partition level table has none.
 */
class TNodeDatabaseMetricsAggregator : public NSysView::IDbDetailedCounters {
public:
    /**
     * @param[in] now Used to differentiate the cumulative counters into per second rates
     *
     * @note The public metrics of the tablet type define what every bucket reports
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
     * Drop everything this tablet contributed to the tree, removing the groups,
     * which are left empty.
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
 * @param[in] isFollowerRole The role of the tablets this instance is fed
 */
TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    bool isFollowerRole
);

} // namespace NKikimr
