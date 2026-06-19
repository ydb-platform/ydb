#include "node_warden_mock.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

void TNodeWardenMockActor::Connect() {
    Y_ABORT_UNLESS(!PipeId);
    PipeId = Register(NTabletPipe::CreateClient(SelfId(), Setup->TabletId, {}));
}

void TNodeWardenMockActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
    YDB_LOG_INFO("Pipe connected",
        {"marker", "NWM02"},
        {"sender", ev->Sender},
        {"pipeId", PipeId},
        {"status", ev->Get()->Status});
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
    YDB_LOG_INFO("Pipe disconnected",
        {"marker", "NWM03"},
        {"sender", ev->Sender},
        {"pipeId", PipeId});
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
