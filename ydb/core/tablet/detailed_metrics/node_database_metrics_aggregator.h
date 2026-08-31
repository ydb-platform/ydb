#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>
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

    /**
     * Pack this instance's own role's view of every table into the wire shape
     * the SysView Service ships to the SysView Processor (step 11/12).
     *
     * Encoding, uniform for a TABLE table's collapse bucket and for every
     * PARTITION leaf (S2): Simple/GAUGE counters are absolute stateful (the
     * full current value, `SwapStatefulCounters` convention — because the
     * receiver clears its per-node Simple state on every message, a gauge
     * dropping to 0 is represented for free). Cumulative/HIST counters are the
     * delta since the previous call for the SAME generation
     * (`AggregateIncrementalCounters` convention), tracked per table partial
     * and per leaf.
     *
     * One TDetailedTableCounters entry is emitted per table this instance
     * still holds a bucket or a leaf for: a TABLE level table fills
     * TableCounters, a PARTITION level table fills one Leaves entry per
     * (tablet_id, follower_id) this instance hosts. The role is NOT part of
     * the payload — nothing here distinguishes a leader instance's message
     * from a follower instance's: the caller (step 12) is the one who knows
     * which role's aggregator it is packing, and stamps that onto the
     * envelope as EDbCountersService (TABLETS / TABLETS_FOLLOWERS).
     *
     * @param[out] out Filled with one entry per table this instance still
     *             holds something for. Not cleared: the caller may be packing
     *             more than one aggregator into the same field.
     * @param[in] generation The SysView Service's current send generation. The
     *            contract is the very same the existing funnel already has
     *            (see SendCounters(), sysview_service.cpp): the SysView
     *            Service only advances the generation once the processor has
     *            confirmed the previous one, so the delta baseline for a new
     *            generation is exactly what was packed as "current" under the
     *            previous generation. Calling Pack() again with the SAME
     *            generation (a retry before confirmation) reproduces the very
     *            same payload byte for byte and does NOT move the baseline.
     */
    virtual void Pack(NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out, ui64 generation) = 0;
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
