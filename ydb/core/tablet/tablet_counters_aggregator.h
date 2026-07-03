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

    // The metrics configuration for the given table.
    struct TTableMetricsConfig {
        ui64 TableId;                                   // ID of the given table (as PathId)
        TString TablePath;                              // Full path for the given table (as /Root/TenantDb/TableName).
        ui64 TableSchemaVersion;                        // Current schema version for the given table.
        ui64 TenantDbSchemaVersion;                     // Current schema version for the parent tenant database.
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel MetricsLevel; // The metrics level for the given table.
        std::optional<TString> MonitoringProjectId;     // Monitoring system ID of the project to send detailed metrics to.
    };

    // Used just as an atomic counter
    struct TInFlightCookie : TThrRefBase {};

    struct TEvTabletAddCounters : public TEventLocal<TEvTabletAddCounters, EvTabletAddCounters> {
        //
        const ui64 TabletID;
        const TTabletTypes::EType TabletType;
        const TPathId TenantPathId;
        TAutoPtr<TTabletCountersBase> ExecutorCounters;
        TAutoPtr<TTabletCountersBase> AppCounters;
        TIntrusivePtr<TInFlightCookie> InFlightCounter;     // Used to detect when previous event has been consumed by the aggregator
        const ui32 FollowerId; // The follower ID, which produced the given set of counters.
        std::optional<TTableMetricsConfig> TableMetricsConfig; // The metrics configuration for the table, which corresponds to the given tablet (if known).

        TEvTabletAddCounters(TIntrusivePtr<TInFlightCookie> inFlightCounter, ui64 tabletID, ui32 followerId, TTabletTypes::EType tabletType, TPathId tenantPathId,
            TAutoPtr<TTabletCountersBase> executorCounters, TAutoPtr<TTabletCountersBase> appCounters, TTableMetricsConfig* tableMetricsConfig = nullptr)
            : TabletID(tabletID)
            , TabletType(tabletType)
            , TenantPathId(tenantPathId)
            , ExecutorCounters(executorCounters)
            , AppCounters(appCounters)
            , InFlightCounter(inFlightCounter)
            , FollowerId(followerId)
            , TableMetricsConfig(tableMetricsConfig ? std::optional<TTableMetricsConfig>(*tableMetricsConfig) : std::nullopt)
        {}
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
        //
        const ui64 TabletID;
        const TTabletTypes::EType TabletType;
        const TPathId TenantPathId;
        const ui32 FollowerId; // The follower ID to forget all counters for.

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

// Translate low level metric values to the corresponding metrics counters.
class ITabletCountersProcessor : public virtual TThrRefBase {
public:
    // Apply tablet counters deltas to the corresponding metrics counters. Only direct counters are updated.
    // To update aggregates use RecalculateAggregatedValues() function.
    virtual void ProcessTabletCounters(ui64 tabletId, const TTabletCountersBase* executorCounters,
        const TTabletCountersBase* applicationCounters, TTabletTypes::EType tabletType ) = 0;

    // Remove counters for a specified tablet.
    virtual void ForgetTablet(ui64 tabletId) = 0;

    // Recalculate the values of all aggregate counters.
    virtual void RecalculateAggregatedValues() = 0;
};

using ITabletCountersProcessorPtr = TIntrusivePtr<ITabletCountersProcessor>;

// Create tablet counters processor for the given tablet type. Creates a subgroup with the label "type"="tabletType".
ITabletCountersProcessorPtr CreateTabletCountersProcessor(
    NMonitoring::TDynamicCounterPtr parentCounterGroup, // The parent counter group to create subgroup in
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
