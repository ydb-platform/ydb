#include "node_warden_mock.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

void TNodeWardenMockActor::Connect() {
    Y_ABORT_UNLESS(!PipeId);
    PipeId = Register(NTabletPipe::CreateClient(SelfId(), Setup->TabletId, {}));
}

void TNodeWardenMockActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
    YDB_LOG_INFO("pipe connected",
        {"Marker", "NWM02"},
        {"Sender", ev->Sender},
        {"PipeId", PipeId},
        {"Status", ev->Get()->Status});
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
    YDB_LOG_INFO("pipe disconnected",
        {"Marker", "NWM03"},
        {"Sender", ev->Sender},
        {"PipeId", PipeId});
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
