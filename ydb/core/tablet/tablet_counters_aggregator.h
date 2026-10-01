#pragma once

////////////////////////////////////////////
#include "defs.h"
#include "tablet_counters.h"

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

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
        EvTabletSetTableInfo,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET_COUNTERS_AGGREGATOR), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET_COUNTERS)");

    // Used just as an atomic counter
    struct TInFlightCookie : TThrRefBase {};

    struct TEvTabletAddCounters : public TEventLocal<TEvTabletAddCounters, EvTabletAddCounters> {
        //
        const ui64 TabletID;
        const NKikimrTabletBase::TTabletTypes::EType TabletType;
        const TPathId TenantPathId;
        TAutoPtr<TTabletCountersBase> ExecutorCounters;
        TAutoPtr<TTabletCountersBase> AppCounters;
        TIntrusivePtr<TInFlightCookie> InFlightCounter;     // Used to detect when previous event has been consumed by the aggregator
        const ui32 FollowerId; // 0 = leader, >0 = replica

        TEvTabletAddCounters(TIntrusivePtr<TInFlightCookie> inFlightCounter, ui64 tabletID, NKikimrTabletBase::TTabletTypes::EType tabletType, TPathId tenantPathId,
            TAutoPtr<TTabletCountersBase> executorCounters, TAutoPtr<TTabletCountersBase> appCounters,
            ui32 followerId = 0)
            : TabletID(tabletID)
            , TabletType(tabletType)
            , TenantPathId(tenantPathId)
            , ExecutorCounters(executorCounters)
            , AppCounters(appCounters)
            , InFlightCounter(inFlightCounter)
            , FollowerId(followerId)
        {}
    };

    // primary user table served by a tablet
    struct TEvTabletSetTableInfo : public TEventLocal<TEvTabletSetTableInfo, EvTabletSetTableInfo> {
        const ui64 TabletID;
        const TPathId TenantPathId;
        const ui32 FollowerId; // 0 = leader, >0 = replica
        const TPathId TableId;
        const TString TablePath;
        const ui64 SchemaVersion;
        // plain to keep this free of the schemeshard proto header
        const ui32 MetricsLevel;

        TEvTabletSetTableInfo(ui64 tabletID, TPathId tenantPathId,
            ui32 followerId, TPathId tableId, const TString& tablePath, ui64 schemaVersion,
            ui32 metricsLevel)
            : TabletID(tabletID)
            , TenantPathId(tenantPathId)
            , FollowerId(followerId)
            , TableId(tableId)
            , TablePath(tablePath)
            , SchemaVersion(schemaVersion)
            , MetricsLevel(metricsLevel)
        {}
    };

    struct TEvTabletAddLabeledCounters : public TEventLocal<TEvTabletAddLabeledCounters, EvTabletAddLabeledCounters> {
        const ui64 TabletID;
        const NKikimrTabletBase::TTabletTypes::EType TabletType;
        TAutoPtr<TTabletLabeledCountersBase> LabeledCounters;
        TIntrusivePtr<TInFlightCookie> InFlightCounter;     // Used to detect when previous event has been consumed by the aggregator
        TEvTabletAddLabeledCounters(TIntrusivePtr<TInFlightCookie> inFlightCounter, ui64 tabletID, NKikimrTabletBase::TTabletTypes::EType tabletType, TAutoPtr<TTabletLabeledCountersBase> labeledCounters)
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
        const NKikimrTabletBase::TTabletTypes::EType TabletType;
        const TPathId TenantPathId;
        const ui32 FollowerId; // 0 = leader, >0 = replica

        TEvTabletCountersForgetTablet(ui64 tabletID, NKikimrTabletBase::TTabletTypes::EType tabletType, TPathId tenantPathId,
            ui32 followerId = 0)
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

////////////////////////////////////////////
void TabletCountersForgetTablet(ui64 tabletId, NKikimrTabletBase::TTabletTypes::EType tabletType, TPathId tenantPathId, bool follower, TActorIdentity identity, ui32 followerId = 0);

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
        NKikimrTabletBase::TTabletTypes::EType tabletType,
        const TActorContext& ctx,
        ui32 version = 1,
        const TString& group = TString(), const ui32 TotalWorkersCount = WORKERS_COUNT);

IActor* CreateClusterLabeledCountersAggregatorActor(
        const TActorId& parentActor,
        NKikimrTabletBase::TTabletTypes::EType tabletType,
        ui32 version = 1,
        const TString& group = TString(), const ui32 TotalWorkersCount = WORKERS_COUNT);

} // namespace NKikimr
