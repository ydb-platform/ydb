#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
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
 * The per-node, per-database, per-role builder of the detailed metrics counter tree.
 *
 * The instance fills the counter group it is handed, which the caller has already
 * scoped to the role of its Tablet Counters Aggregator actor:
 *
 *     ydb_detailed_raw                        (private, created by the caller)
 *       |
 *       +-- the target group of BOTH instances
 *           database=<database path>
 *             table=<table path relative to the database>
 *               Table level:     the collapsed counters of the table (leaders only)
 *               Partition level: detailed_metrics=per_partition
 *                                  tablet_id=<id>
 *                                    follower_id=<n>
 *
 * Every group, which holds counters above, holds them as a
 * type=<tablet type>/category=executor|app subtree of low level counter aggregates,
 * the very same layout as the node wide "tablets" group.
 *
 */
class TNodeDatabaseMetricsAggregator : public TThrRefBase {
public:
    /**
     * @param[in] now Used to differentiate the cumulative counters into per second rates
     *
     * @warning Every named counter of the two counter sets is published as its own
     *          series in every bucket, so the caller decides the cardinality
     */
    virtual void AddCounters(
        const TDetailedMetricsTableInfo& table,
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
 * @param[in] targetCounterGroup The group to fill, already scoped to the role
 * @param[in] isFollowerRole The role of the tablets this instance is fed
 */
TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    bool isFollowerRole
);

} // namespace NKikimr
