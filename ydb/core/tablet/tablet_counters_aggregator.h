#pragma once

////////////////////////////////////////////
#include "defs.h"
#include "tablet_counters.h"

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

#include <ydb/core/base/blobstorage.h>
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

        TEvTabletAddCounters(TIntrusivePtr<TInFlightCookie> inFlightCounter, ui64 tabletID, TTabletTypes::EType tabletType, TPathId tenantPathId,
            TAutoPtr<TTabletCountersBase> executorCounters, TAutoPtr<TTabletCountersBase> appCounters)
            : TabletID(tabletID)
            , TabletType(tabletType)
            , TenantPathId(tenantPathId)
            , ExecutorCounters(executorCounters)
            , AppCounters(appCounters)
            , InFlightCounter(inFlightCounter)
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

        TEvTabletCountersForgetTablet(ui64 tabletID, TTabletTypes::EType tabletType, TPathId tenantPathId)
            : TabletID(tabletID)
            , TabletType(tabletType)
            , TenantPathId(tenantPathId)
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
void TabletCountersForgetTablet(ui64 tabletId, TTabletTypes::EType tabletType, TPathId tenantPathId, bool follower, TActorIdentity identity);

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
