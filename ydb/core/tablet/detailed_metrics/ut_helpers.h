#pragma once

#include "node_database_metrics_aggregator.h"

#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr {

namespace NDetailedMetricsTests {

/**
 * Normalizes the given JSON to be well formatted with all keys sorted.
 *
 * @warning This function sorts only maps by the key value. It does not sort
 *          items in arrays at all. Luckily, all counters and groups in TDynamicCounters
 *          are stored in SORTED maps, which means that the array of sensors
 *          is always inherently sorted in a stable order. This makes it safe
 *          to compare sensor arrays directly without sorting them.
 *
 * @param[in] jsonString The JSON to normalize (as a string)
 *
 * @return The corresponding normalized JSON
 */
TString NormalizeJson(const TString& jsonString);

////////////////////////////////////////////////////////////////////////////////
// TFakeTablet, Pack() helpers: shared between node_database_metrics_
// aggregator_ut.cpp and processor_database_metrics_aggregator_ut.cpp, which
// both drive the aggregators through the very same "real tablet reports,
// then Pack()" transport (step 09's processor test feeds the packed protos
// straight into the processor aggregator instead of asserting on them
// directly, the way the node's own Pack tests do).

/**
 * The default table identity TFakeTablet::Report() reports under, shared so
 * that a bare tablet.Report(aggregator, level, now) call means the same
 * table/type in both test files.
 */
extern const TPathId TABLE_ID;
extern const TString TABLE_PATH;
extern const TString RELATIVE_TABLE_PATH;
extern const TTabletTypes::EType TABLET_TYPE;

enum ESimpleCounter : ui32 {
    DB_UNIQUE_ROWS_TOTAL = 0,
    DB_UNIQUE_DATA_BYTES = 1,
    // Absent from the DataShard allow-list (ydb/core/protos/counters_detailed_datashard.proto),
    // used to verify Initialize()'s nameFilter is honored
    NOT_IN_ALLOW_LIST = 2,
};

enum ECumulativeCounter : ui32 {
    CONSUMED_CPU = 0,
};

enum EAppCumulativeCounter : ui32 {
    ENGINE_HOST_ROW_UPDATES = 0,
    ENGINE_HOST_ROW_UPDATE_BYTES = 1,
};

/**
 * A small stand-in for the low level counters of Data Shard. The real counter set
 * has hundreds of counters, which would make the assertions unreadable without
 * covering anything, which is not covered by these few counters.
 *
 * @note Reporting goes through MakeDiffForAggr()/RememberCurrentStateAsBaseline(),
 *       exactly like the Executor does it, so what the aggregator sees is what it
 *       sees in production: the simple counters are absolute, the cumulative ones
 *       are the delta since the previous report of THIS tablet, and the integral
 *       percentile counters are absolute.
 */
struct TFakeTablet {
    TFakeTablet(ui64 tabletId, ui32 followerId);

    TFakeTablet& SetSimple(ESimpleCounter counter, ui64 value);
    TFakeTablet& AddCumulative(ECumulativeCounter counter, ui64 delta);
    TFakeTablet& AddAppCumulative(EAppCumulativeCounter counter, ui64 delta);

    /**
     * Send everything accumulated since the previous report, the way the Executor does.
     */
    void Report(
        const TNodeDatabaseMetricsAggregatorPtr& aggregator,
        EDetailedMetricsLevel level,
        TInstant now,
        const TString& tablePath = TABLE_PATH,
        TTabletTypes::EType tabletType = TABLET_TYPE
    );

    const ui64 TabletId;
    const ui32 FollowerId;

    TTabletCountersBase ExecutorCounters;
    TTabletCountersBase AppCounters;

    // The state as of the previous report, subtracted from the cumulative counters
    TTabletCountersBase ExecutorBaseline;
    TTabletCountersBase AppBaseline;
};

/**
 * A template TTabletCountersBase with TFakeTablet's OWN Executor category
 * layout ({DbUniqueRowsTotal, DbUniqueDataBytes, NotInTheAllowList} /
 * {ConsumedCPU} / {HIST(ConsumedCPU)}), for
 * TProcessorDatabaseMetricsAggregator's executorCountersTemplate parameter.
 *
 * @note The wire format is positional (index, not name), so a receiver must
 *       Initialize() off a counter set with the very same names IN THE SAME
 *       ORDER the sender used, or it reads someone else's value at that
 *       index (padded-with-zeros past the sender's own count, never a crash,
 *       but silently wrong). In production this holds for free: node and
 *       processor both call NTabletFlatExecutor::TExecutorCounters(). In a
 *       test built on TFakeTablet's toy layout instead, the processor must
 *       be handed THIS template rather than the real one — mismatched
 *       layouts between the two sides here would not fail loudly, they
 *       would just misattribute test data (see processor_database_metrics_
 *       aggregator_ut.cpp, which sticks to Executor-sourced public metrics
 *       for exactly this reason: the App category has no such injection
 *       point — TProcessorDatabaseMetricsAggregator always initializes it
 *       from the real CreateAppCountersByTabletType(), per the step 09
 *       plan — so an App-sourced metric is not safe to assert on here).
 */
THolder<TTabletCountersBase> MakeFakeExecutorCountersTemplate();

/**
 * Pack a single instance's own tables into a fresh field, the way the SysView
 * Service would pack one role's stream for one message.
 */
NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters> PackOnce(
    const TNodeDatabaseMetricsAggregatorPtr& aggregator,
    ui64 generation
);

/**
 * @return The packed entry of the table at the given (unstripped) path, or
 *         nullptr if this instance packed nothing for it
 */
const NKikimrSysView::TDetailedTableCounters* FindPackedTable(
    const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables,
    const TString& tablePath = TABLE_PATH
);

/**
 * @return The packed leaf of the given (tabletId, followerId), or nullptr if
 *         this entry carries none
 */
const NKikimrSysView::TDetailedTableCounters::TLeaf* FindPackedLeaf(
    const NKikimrSysView::TDetailedTableCounters& table,
    ui64 tabletId,
    ui32 followerId
);

} // namespace NDetailedMetricsTests

} // namespace NKikimr
