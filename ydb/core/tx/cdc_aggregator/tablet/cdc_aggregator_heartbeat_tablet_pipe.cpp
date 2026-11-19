#include "cdc_aggregator_heartbeat_tablet_pipe.h"

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/actor.h>

#include <utility>

namespace NKikimr::NCdcAggregator {

TCdcAggregatorHeartbeatTabletPipe::TCdcAggregatorHeartbeatTabletPipe(ui64 tabletId)
    : TabletId(tabletId)
{
}

void TCdcAggregatorHeartbeatTabletPipe::Send(THolder<IEventBase> event) {
    const auto& ctx = TActivationContext::AsActorContext();
    if (!Pipe) {
        Pipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, TabletId));
    }
    NTabletPipe::SendData(ctx.SelfID, Pipe, std::move(event));
}

bool TCdcAggregatorHeartbeatTabletPipe::Reset(const TActorId& lostPipe) {
    if (!Pipe || Pipe != lostPipe) {
        return false;
    }
    Pipe = {};
    return true;
}

void TCdcAggregatorHeartbeatTabletPipe::Close() {
    if (Pipe) {
        NTabletPipe::CloseClient(TActivationContext::AsActorContext(), Pipe);
        Pipe = {};
    }
}

} // namespace NKikimr::NCdcAggregator
