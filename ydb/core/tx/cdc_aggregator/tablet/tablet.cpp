#include "tablet.h"
#include "tablet_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

TCdcAggregator::TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
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
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvCdcAggregator::TEvWriteHeartbeats, Handle);
        HFunc(TEvCdcAggregator::TEvPartitioningChanged, Handle);
        HFunc(TEvCdcAggregator::TEvDescribeTracking, Handle);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            YDB_LOG_WARN("Unexpected event",
                {"tabletId", TabletID()},
                {"eventType", ev->GetTypeRewrite()});
        }
    }
}

void TCdcAggregator::SwitchToWork(const TActorContext& ctx) {
    SignalTabletActive(ctx);
    Become(&TThis::StateWork);
}

void TCdcAggregator::Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"tabletId", TabletID()},
        {"ev", ev->Get()->ToString()});

    auto result = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeatsResult>();
    result->Record.SetStatus(NKikimrCdcAggregator::TEvWriteHeartbeatsResult::NOT_READY);
    result->Record.SetOrigin(TabletID());
    ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
}

void TCdcAggregator::Handle(TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"tabletId", TabletID()},
        {"ev", ev->Get()->ToString()});

    auto result = MakeHolder<TEvCdcAggregator::TEvPartitioningChangedAck>();
    result->Record.SetStatus(NKikimrCdcAggregator::TEvPartitioningChangedAck::NOT_READY);
    result->Record.SetOrigin(TabletID());
    ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
}

void TCdcAggregator::Handle(TEvCdcAggregator::TEvDescribeTracking::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"tabletId", TabletID()},
        {"ev", ev->Get()->ToString()});

    ctx.Send(ev->Sender, new TEvCdcAggregator::TEvDescribeTrackingResult(), 0, ev->Cookie);
}

IActor* CreateCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TCdcAggregator(tablet, info);
}

} // namespace NKikimr::NCdcAggregator
