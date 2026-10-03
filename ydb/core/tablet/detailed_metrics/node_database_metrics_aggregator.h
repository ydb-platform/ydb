#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

#include <array>

namespace NKikimr {

struct TDetailedMetricsDescriptor;

/**
 * Serializes the calls into the TNodeDatabaseMetricsAggregator instances of the node,
 * which come from two sides:
 * - the Tablet Counters Aggregator actors, the leader and the follower one: AddCounters(),
 *   ForgetTablet() and RecalculateAllCounters(), the last of which refreshes the low level
 *   counters of the TABLE buckets in the counter tree;
 * - the SysView Service actor: Pack(), which is a writer as well, as it drains the rate and
 *   the increment deltas of the buckets and the final values of the retired ones.
 *
 * Each of these methods takes the lock on its own, for its whole call: the two sides run
 * on different threads and share the tables, the buckets and their values, so they race
 * without it.
 *
 * @note A single lock for all the databases and both roles of the node. It is recursive
 *       (a TMutex), though nothing re-enters it.
 *
 * @note The readers of the counter tree do NOT take it: the monitoring pages walk the tree
 *       under the locks of its counter groups only, so they may see a HIST(x) aggregate
 *       of a TABLE bucket midway through RecalculateAllCounters(). A reader, which holds
 *       the lock across its whole traversal (as the tests do), sees whole aggregates.
 *
 * @note It is always taken before the locks of the counter groups, never while one of them
 *       is held.
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

/**
 * Get the descriptor of the public detailed metrics of a tablet type, nullptr if the type has none,
 * see GetDetailedMetricsDescriptor().
 *
 * @note The descriptor must outlive every aggregator, which gets it.
 */
using TDetailedMetricsDescriptorGetter = const TDetailedMetricsDescriptor* (*)(TTabletTypes::EType tabletType);

/**
 * The same as above, but the public metrics of the tablet types are described by the given
 * function rather than by GetDetailedMetricsDescriptor().
 *
 * @note For the tests: a synthetic descriptor reaches the cases, which the descriptors
 *       of the production tablet types do not (e.g. an empty allow-list of a category
 *       of the low level counters, or a descriptor with errors).
 */
TNodeDatabaseMetricsAggregatorPtr CreateNodeDatabaseMetricsAggregator(
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    const TString& databasePath,
    bool isFollowerRole,
    TDetailedMetricsDescriptorGetter getDescriptor
);

/**
 * Get the sizes of the simple, the cumulative and the percentile counter arrays of the layout,
 * which the aggregate of the application counters of the TABLE bucket of the table is built on:
 * the application counter template of the tablet type, if it is a prefix of the reported layout,
 * which holds every published counter, the reported layout otherwise.
 *
 * @note For the tests, the published counters are the same either way.
 *
 * @param[in] aggregator The aggregator created by CreateNodeDatabaseMetricsAggregator()
 * @param[in] tablePath The full path of the table, as the tablets report it
 *
 * @return The sizes, or Nothing() if the table has no TABLE bucket
 */
TMaybe<std::array<ui32, 3>> GetTableBucketAppLayoutSizes(
    const TNodeDatabaseMetricsAggregator& aggregator,
    const TString& tablePath
);

} // namespace NKikimr
