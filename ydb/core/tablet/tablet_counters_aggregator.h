#pragma once

////////////////////////////////////////////
#include "defs.h"
#include "tablet_counters.h"

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/labeled_counters.pb.h>
#include <ydb/core/protos/tablet_counters_aggregator.pb.h>
#include <ydb/core/sys_view/common/events.h>

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
TActorId MakeTabletCountersAggregatorID(ui32 node, bool follower = false);


static const ui32 WORKERS_COUNT = 0;

////////////////////////////////////////////
struct TEvTabletCounters {
    //
    enum EEv {
        EvTabletAddCounters = EventSpaceBegin(TKikimrEvents::ES_TABLET_COUNTERS_AGGREGATOR),
        EvDeprecated1,
        EvTabletCountersForgetTablet,
        EvTabletCountersRequest,
        EvTabletCountersResponse,
        EvTabletAddLabeledCounters,
        EvTabletLabeledCountersRequest,
        EvTabletLabeledCountersResponse,
        EvRemoveDatabase,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET_COUNTERS_AGGREGATOR), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET_COUNTERS)");

    /**
     * The metrics configuration for the given table.
     */
    struct TTableMetricsConfig {
        /**
         * The ID of the given table (as PathId).
         */
        ui64 TableId;

        /**
         * The full path for the given table (as /Root/TenantDb/TableName).
         */
        TString TablePath;

        /**
         * The current schema version for the given table.
         *
         * @note This field is used to determine changes to the MetricsLevel,
         *       TableId and TablePath fields.
         */
        ui64 TableSchemaVersion;

        /**
         * The current schema version for the parent tenant database.
         *
         * @note This field is used to determine changes to the MonitoringProjectId field.
         */
        ui64 TenantDbSchemaVersion;

        /**
         * The metrics level for the given table.
         */
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel MetricsLevel;

        /**
         * The ID of the project in the Monitoring system, where the detailed metrics
         * should be sent to.
         */
        std::optional<TString> MonitoringProjectId;
    };

    // Used just as an atomic counter
    struct TInFlightCookie : TThrRefBase {};

    struct TEvTabletAddCounters : public TEventLocal<TEvTabletAddCounters, EvTabletAddCounters> {
        const ui64 TabletID;
        const TTabletTypes::EType TabletType;
        const TPathId TenantPathId;
        TAutoPtr<TTabletCountersBase> ExecutorCounters;
        TAutoPtr<TTabletCountersBase> AppCounters;
        TIntrusivePtr<TInFlightCookie> InFlightCounter;     // Used to detect when previous event has been consumed by the aggregator

        /**
         * The follower ID, which produced the given set of counters.
         */
        const ui32 FollowerId;

        /**
         * The metrics configuration for the table, which corresponds
         * to the given tablet (if known).
         */
        std::optional<TTableMetricsConfig> TableMetricsConfig;

        TEvTabletAddCounters(
            TIntrusivePtr<TInFlightCookie> inFlightCounter,
            ui64 tabletID,
            ui32 followerId,
            TTabletTypes::EType tabletType,
            TPathId tenantPathId,
            TAutoPtr<TTabletCountersBase> executorCounters,
            TAutoPtr<TTabletCountersBase> appCounters,
            TTableMetricsConfig* tableMetricsConfig = nullptr
        )
            : TabletID(tabletID)
            , TabletType(tabletType)
            , TenantPathId(tenantPathId)
            , ExecutorCounters(executorCounters)
            , AppCounters(appCounters)
            , InFlightCounter(inFlightCounter)
            , FollowerId(followerId)
        {
            if (tableMetricsConfig != nullptr) {
                // Make a full copy of the configuration to avoid locking between actors
                TableMetricsConfig = (*tableMetricsConfig);
            }
        }
    };

    struct TEvTabletAddLabeledCounters : public TEventLocal<TEvTabletAddLabeledCounters, EvTabletAddLabeledCounters> {
        const ui64 TabletID;
        const TTabletTypes::EType TabletType;
        TAutoPtr<TTabletLabeledCountersBase> LabeledCounters;
        TIntrusivePtr<TInFlightCookie> InFlightCounter;     // Used to detect when previous event has been consumed by the aggregator
        TEvTabletAddLabeledCounters(TIntrusivePtr<TInFlightCookie> inFlightCounter, ui64 tabletID, TTabletTypes::EType tabletType, TAutoPtr<TTabletLabeledCountersBase> labeledCounters)
            : TabletID(tabletID)
            , TabletType(tabletType)
            , LabeledCounters(labeledCounters)
            , InFlightCounter(inFlightCounter)
        {}
    };

    //
    struct TEvTabletCountersForgetTablet : public TEventLocal<TEvTabletCountersForgetTablet, EvTabletCountersForgetTablet> {
        const ui64 TabletID;
        const TTabletTypes::EType TabletType;
        const TPathId TenantPathId;

        /**
         * The follower ID, for which to forget all counters.
         */
        const ui32 FollowerId;

        TEvTabletCountersForgetTablet(ui64 tabletID, ui32 followerId, TTabletTypes::EType tabletType, TPathId tenantPathId)
            : TabletID(tabletID)
            , TabletType(tabletType)
            , TenantPathId(tenantPathId)
            , FollowerId(followerId)
        {}
    };

    //
    struct TEvTabletCountersRequest : public TEventPB<TEvTabletCountersRequest, NKikimrTabletCountersAggregator::TEvTabletCountersRequest, EvTabletCountersRequest> {
    };

    struct TEvTabletCountersResponse : public TEventPB<TEvTabletCountersResponse, NKikimrTabletCountersAggregator::TEvTabletCountersResponse, EvTabletCountersResponse> {
    };

    //
    struct TEvTabletLabeledCountersRequest : public TEventPB<TEvTabletLabeledCountersRequest, NKikimrLabeledCounters::TEvTabletLabeledCountersRequest, EvTabletLabeledCountersRequest> {
    };

    struct TEvTabletLabeledCountersResponse : public TEventPB<TEvTabletLabeledCountersResponse, NKikimrLabeledCounters::TEvTabletLabeledCountersResponse, EvTabletLabeledCountersResponse> {
    };

    struct TEvRemoveDatabase : public TEventLocal<TEvRemoveDatabase, EvRemoveDatabase> {
        const TString DbPath;
        const TPathId PathId;

        TEvRemoveDatabase(const TString& dbPath, TPathId pathId)
            : DbPath(dbPath)
            , PathId(pathId)
        {}
    };

};

struct TTabletLabeledCountersResponseContext {
    NKikimrLabeledCounters::TEvTabletLabeledCountersResponse& Response;
    THashMap<TStringBuf, ui32> NamesToId;

    TTabletLabeledCountersResponseContext(NKikimrLabeledCounters::TEvTabletLabeledCountersResponse& response);

    ui32 GetNameId(TStringBuf name);
};

/**
 * The generic interface for the class, which translates low level metric values
 * (both executor counters and application counters) to the corresponding metrics counters.
 *
 * @note Essentially, this is a way to expose the TTabletCountersForTabletType class
 *       from the Tablets Counters Aggregator implementation to the outside so that
 *       it can be reused for other purposes, more specifically, to manage and to aggregate
 *       detailed metrics on the same node.
 */
class ITabletCountersProcessor : public virtual TThrRefBase {
public:
    /**
     * Process a single batch of tablet counters and apply them to the corresponding
     * metrics counters.
     *
     * @note This function assumes that the given counters are differential
     *       (as a delta) from the previous batch.
     *
     * @warning This function updates only the values of direct counters, which derive
     *          their values directly from the tablet counters. Any aggregate values
     *          (e.g. "SUM(DbUniqueRowsTotal)" or "HIST(ConsumedCPU)") are not updated.
     *          To see the updated values, the RecalculateAggregatedValues() function
     *          must be called explicitly.
     *
     * @param[in] tabletId The ID of the tablet
     * @param[in] executorCounters The executor counters to process (differential)
     * @param[in] applicationCounters The application counters to process (differential)
     * @param[in] tabletType The type of the tablet
     */
    virtual void ProcessTabletCounters(
        ui64 tabletId,
        const TTabletCountersBase* executorCounters,
        const TTabletCountersBase* applicationCounters,
        TTabletTypes::EType tabletType
    ) = 0;

    /**
     * Remove the values supplied by the given tablet from all counters.
     *
     * @param[in] tabletId The ID of the tablet
     */
    virtual void ForgetTablet(ui64 tabletId) = 0;

    /**
     * Recalculate the values of all counters, which are aggregates of low level
     * counters reported by the tablet, for example, min, max, etc.
     */
    virtual void RecalculateAggregatedValues() = 0;
};

using ITabletCountersProcessorPtr = TIntrusivePtr<ITabletCountersProcessor>;

/**
 * Create the processor of tablet counters for the given tablet type.
 *
 * @note This functions creates a subgroup with the label "type"="tabletType"
 *       under the parent counter group (specified by parentCounterGroup).
 *       For example, for Data Shard counters, the "type"="DataShard" label
 *       is used.
 *
 * @param[in] parentCounterGroup The parent counter group for all tablet counters
 * @param[in] tabletType The type of the tablet to be handled by this processor
 *
 * @return The processor of tablet counters for the given tablet type
 */
ITabletCountersProcessorPtr CreateTabletCountersProcessor(
    NMonitoring::TDynamicCounterPtr parentCounterGroup,
    TTabletTypes::EType tabletType
);

////////////////////////////////////////////
void TabletCountersForgetTablet(ui64 tabletId, ui32 followerId, TTabletTypes::EType tabletType, TPathId tenantPathId, bool follower, TActorIdentity identity);

TStringBuf GetHistogramAggregateSimpleName(TStringBuf name);
bool IsHistogramAggregateSimpleName(TStringBuf name);

////////////////////////////////////////////
TIntrusivePtr<NSysView::IDbCounters> CreateTabletDbCounters(
    ::NMonitoring::TDynamicCounterPtr externalGroup,
    ::NMonitoring::TDynamicCounterPtr internalGroup,
    THolder<TTabletCountersBase> executorCounters);

////////////////////////////////////////////
IActor* CreateTabletCountersAggregator(bool follower);


////////////////////////////////////////////
//will create actor that aggregate LabeledCounters from all nodes and reports them as TEvTabletLabeledCountersResponse to parentActor
TActorId CreateClusterLabeledCountersAggregator(
        const TActorId& parentActor,
        TTabletTypes::EType tabletType,
        const TActorContext& ctx,
        ui32 version = 1,
        const TString& group = TString(), const ui32 TotalWorkersCount = WORKERS_COUNT);

IActor* CreateClusterLabeledCountersAggregatorActor(
        const TActorId& parentActor,
        TTabletTypes::EType tabletType,
        ui32 version = 1,
        const TString& group = TString(), const ui32 TotalWorkersCount = WORKERS_COUNT);

} // namespace NKikimr
