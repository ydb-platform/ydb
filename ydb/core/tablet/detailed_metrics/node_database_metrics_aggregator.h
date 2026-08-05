#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {

/**
 * The per-table detailed metrics settings, as stored in the schema.
 *
 * @note The enum values live in the message class, NOT in the enum type, so they are
 *       spelled TDetailedMetricsSettings::MetricsLevelTable and the like.
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
 * The per-node, per-database builder of the detailed metrics counter tree:
 *
 *     <target group>
 *       database=<database path>
 *         monitoring_project_id=<id>            (only if non-empty)
 *           table=<table path relative to the database>
 *             role=leader                       (Table level only)
 *             role=follower                     (Table level only)
 *             detailed_metrics=per_partition    (Partition level only)
 *               tablet_id=<id>
 *                 follower_id=<n>
 *
 * Each of the leaf groups above holds a type=<tablet type>/category=executor|app subtree
 * of low level counter aggregates, the very same layout as the node wide "tablets" group.
 *
 * @note At the Table level the tablets of a table are collapsed into two buckets on
 *       the node. At the Partition level nothing is collapsed: a (table, tablet, follower)
 *       leaf is owned by exactly one node, so the consumer unions the leaves across
 *       the nodes and never sums them.
 */
class TNodeDatabaseMetricsAggregator : public TThrRefBase {
public:
    /**
     * @param[in] now Used to differentiate the cumulative counters into per second rates
     *
     * @warning Every named counter of the two counter sets is published as its own
     *          series in every bucket, so the caller decides the cardinality. See the
     *          TODO in TCountersBucket::Apply() before enabling the Partition level.
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
     */
    virtual void ForgetTablet(const TPathId& tableId, ui64 tabletId, ui32 followerId) = 0;

    virtual void RecalculateAllCounters() = 0;
};

using TNodeDatabaseMetricsAggregatorPtr = TIntrusivePtr<TNodeDatabaseMetricsAggregator>;

TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    const TString& monitoringProjectId
);

} // namespace NKikimr
