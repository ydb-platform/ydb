#include "packet.h"
#include "interconnect_counters.h"

#include <ydb/library/actors/core/probes.h>

#include <util/system/datetime.h>

LWTRACE_USING(ACTORLIB_PROVIDER);

TTcpPacketOutTask::TTcpPacketOutTask(const TSessionParams& params, NInterconnect::TOutgoingStream& outgoingStream,
    NInterconnect::TOutgoingStream& xdcStream, NActors::IInterconnectMetrics* metrics)
    : Params(params)
    , OutgoingStream(outgoingStream)
    , XdcStream(xdcStream)
    , HeaderBookmark(OutgoingStream.Bookmark(sizeof(TTcpPacketHeader_v2)))
    , Metrics(metrics)
{}

void TTcpPacketOutTask::UpdateIcQueueDuration(const TEventHolder& event) {
    if (event.EnqueueTime) {
        TDuration duration = NActors::TlsActivationContext->Now() - event.EnqueueTime;
        Metrics->UpdateIcQueueTimeHistogram(duration.MicroSeconds());
    }
}

ui32 TEventHolder::Fill(IEventHandle& ev) {
    Serial = 0;
    Descr.Type = ev.Type;
    Descr.Flags = ev.Flags;
    Descr.Recipient = ev.Recipient;
    Descr.Sender = ev.Sender;
    Descr.Cookie = ev.Cookie;
    ForwardRecipient = ev.GetForwardOnNondeliveryRecipient();
    EventActuallySerialized = 0;
    Descr.Checksum = 0;
    ZcTransferId = 0;

    if (ev.HasBuffer()) {
        Buffer = ev.ReleaseChainBuffer();
        EventSerializedSize = Buffer->GetSize();
    } else if (ev.HasEvent()) {
        Event.Reset(ev.ReleaseBase());
        EventSerializedSize = Event->CalculateSerializedSize();
        Y_ABORT_UNLESS(Event->IsSerializable());
    } else {
        EventSerializedSize = 0;
    }

    return EventSerializedSize;
}

ui32 TEventHolder::Fill(IEventHandle& ev, TInstant now) {
    EnqueueTime = now;
    return Fill(ev);
}