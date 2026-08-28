#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::HandlePushMetrics() {
        if (IsConnected) {
            auto takeDelta = [](const NMonitoring::TDynamicCounters::TCounterPtr& counter, ui64& last) -> ui64 {
                const ui64 value = counter ? ui64(*counter) : 0;
                return value - std::exchange(last, value);
            };

            const ui64 bytesRead = BytesRead - std::exchange(LastBytesRead, BytesRead);
            const ui64 bytesWritten = BytesWritten - std::exchange(LastBytesWritten, BytesWritten);
            auto event = std::make_unique<TEvBlobDepot::TEvPushMetrics>(bytesRead, bytesWritten);
            auto& record = event->Record;
            record.SetNodeId(SelfId().NodeId());
            record.SetS3GetsOk(takeDelta(S3GetsOk, LastS3GetsOk));
            record.SetS3GetsError(takeDelta(S3GetsError, LastS3GetsError));
            record.SetS3GetsBytes(takeDelta(S3GetBytesOk, LastS3GetBytesOk));
            record.SetS3GetsSlowDown(takeDelta(S3GetsSlowDown, LastS3GetsSlowDown));
            record.SetS3GetThrottleActivations(takeDelta(S3GetThrottleActivations, LastS3GetThrottleActivations));
            record.SetS3GetsInFlight(S3GetsInFlight);
            record.SetS3GetsMaxInFlight(CurrentMaxS3GetsInFlight);
            record.SetS3GetsPendingQueueSize(PendingS3Reads.size());
            NTabletPipe::SendData(SelfId(), PipeId, event.release());
        }

        TActivationContext::Schedule(TDuration::MilliSeconds(2500), new IEventHandle(TEvPrivate::EvPushMetrics, 0, SelfId(),
            {}, nullptr, 0));
    }

} // NKikimr::NBlobDepot
