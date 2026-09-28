#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::HandlePushMetrics() {
        if (IsConnected) {
            const ui64 bytesRead = BytesRead - std::exchange(LastBytesRead, BytesRead);
            const ui64 bytesWritten = BytesWritten - std::exchange(LastBytesWritten, BytesWritten);
            auto event = std::make_unique<TEvBlobDepot::TEvPushMetrics>(bytesRead, bytesWritten);
            event->Record.SetNodeId(SelfId().NodeId());
            NTabletPipe::SendData(SelfId(), PipeId, event.release());
        }

        TActivationContext::Schedule(TDuration::MilliSeconds(2500), new IEventHandle(TEvPrivate::EvPushMetrics, 0, SelfId(),
            {}, nullptr, 0));
    }

} // NKikimr::NBlobDepot
