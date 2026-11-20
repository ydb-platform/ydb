#include "cdc_aggregator_heartbeat_broadcaster.h"
#include "tablet.h"
#include "tablet_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

TCdcAggregator::TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info)
    : TTabletActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
}

void TCdcAggregator::Die(const TActorContext& ctx) {
    if (CdcAggregatorHeartbeatBroadcaster) {
        ctx.Send(CdcAggregatorHeartbeatBroadcaster, new TEvents::TEvPoison);
    }
    IActor::Die(ctx);
}

void TCdcAggregator::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TCdcAggregator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void TCdcAggregator::OnActivateExecutor(const TActorContext& ctx) {
    RunTxInitSchema(ctx);
}

void TCdcAggregator::DefaultSignalTabletActive(const TActorContext&) {
}

STFUNC(TCdcAggregator::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TCdcAggregator::StateWork) {
    YDB_LOG_CREATE_CONTEXT({"tabletId", TabletID()});
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvCdcAggregator::TEvConfigureTracking, Handle);
        HFunc(TEvCdcAggregator::TEvWriteHeartbeats, Handle);
        HFunc(TEvCdcAggregator::TEvPartitioningChanged, Handle);
        HFunc(TEvCdcAggregator::TEvDescribeTracking, Handle);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            YDB_LOG_WARN("Unexpected event",
                {"eventType", ev->GetTypeRewrite()});
        }
    }
}

void TCdcAggregator::SwitchToWork(const TActorContext& ctx) {
    SignalTabletActive(ctx);
    Become(&TThis::StateWork);
}

void TCdcAggregator::BroadcastHeartbeat(const NPQ::THeartbeat& heartbeat, const TActorContext& ctx) {
    if (!CdcAggregatorHeartbeatBroadcaster) {
        CdcAggregatorHeartbeatBroadcaster = ctx.Register(CreateCdcAggregatorHeartbeatBroadcaster(CdcAggregatorStream->GetTopic()));
    }
    ctx.Send(CdcAggregatorHeartbeatBroadcaster, new TEvCdcAggregatorHeartbeatBroadcaster::TEvBroadcastHeartbeat(heartbeat));
}

void TCdcAggregator::Handle(TEvCdcAggregator::TEvDescribeTracking::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"ev", ev->Get()->ToString()});

    auto result = MakeHolder<TEvCdcAggregator::TEvDescribeTrackingResult>();
    if (CdcAggregatorStream) {
        result->Record = CdcAggregatorStreamProtoConverter.SerializeDescribeTrackingResult(*CdcAggregatorStream);
    }
    ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
}

IActor* CreateCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TCdcAggregator(tablet, info);
}

} // namespace NKikimr::NCdcAggregator
