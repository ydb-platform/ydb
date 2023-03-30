#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::HandlePushMetrics() {
        if (IsConnected) {
            const ui64 bytesRead = BytesRead - std::exchange(LastBytesRead, BytesRead);
            const ui64 bytesWritten = BytesWritten - std::exchange(LastBytesWritten, BytesWritten);
            NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvPushMetrics(bytesRead, bytesWritten));
        }

        TActivationContext::Schedule(TDuration::MilliSeconds(2500), new IEventHandle(TEvPrivate::EvPushMetrics, 0, SelfId(),
            {}, nullptr, 0));
    }

} // NKikimr::NBlobDepot
