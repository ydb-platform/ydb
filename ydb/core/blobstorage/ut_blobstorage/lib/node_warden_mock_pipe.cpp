#include "node_warden_mock.h"

void TNodeWardenMockActor::Connect() {
    Y_ABORT_UNLESS(!PipeId);
    PipeId = Register(NTabletPipe::CreateClient(SelfId(), Setup->TabletId, {}));
}

void TNodeWardenMockActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
    STLOG(PRI_INFO, BS_NODE, NWM02, "pipe connected", (Sender, ev->Sender), (PipeId, PipeId), (Status, ev->Get()->Status));
    if (ev->Sender == PipeId) {
        Y_ABORT_UNLESS(!IsPipeConnected);
        if (ev->Get()->Status == NKikimrProto::OK) {
            IsPipeConnected = true;
            OnConnected();
        } else {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeId);
            ScheduleReconnect();
        }
    }
}

void TNodeWardenMockActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
    STLOG(PRI_INFO, BS_NODE, NWM03, "pipe disconnected", (Sender, ev->Sender), (PipeId, PipeId));
    if (ev->Sender == PipeId) {
        IsPipeConnected = false;
        ScheduleReconnect();
    }
}

void TNodeWardenMockActor::ScheduleReconnect() {
    Y_ABORT_UNLESS(!IsPipeConnected);
    PipeId = {};
    Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
}
