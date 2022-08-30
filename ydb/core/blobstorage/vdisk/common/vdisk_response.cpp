#include "vdisk_response.h"
#include "vdisk_events.h"
#include <ydb/core/base/interconnect_channels.h>
#include <util/system/datetime.h>

namespace NKikimr {

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie) {
    ui32 channel = TInterconnectChannels::IC_BLOBSTORAGE;
    if (TEvVResultBase *base = dynamic_cast<TEvVResultBase *>(ev)) {
        channel = base->GetChannelToSend();
    }
    SendVDiskResponse(ctx, recipient, ev, cookie, channel);
}

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, ui32 channel) {
    switch (const ui32 type = ev->Type()) {
#define HANDLE_EVENT(T, EV) \
        case TEvBlobStorage::T::EventType: { \
            TEvBlobStorage::T *event = static_cast<TEvBlobStorage::T *>(ev); \
            const double usPerCycle = 1000000.0 / NHPTimer::GetCyclesPerSecond(); \
            event->Record.MutableTimestamps()->SetSentByVDiskUs(GetCycleCountFast() * usPerCycle); \
            break; \
        }

        HANDLE_EVENT(TEvVPutResult, EvVPutResultSent)
        HANDLE_EVENT(TEvVMultiPutResult, EvVMultiPutResultSent)
        HANDLE_EVENT(TEvVGetResult, EvVGetResultSent)

#undef HANDLE_EVENT

        case TEvBlobStorage::EvVAssimilateResult: // override channel for assimilation result
            channel = TInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA;
            break;
    }

    auto event = std::make_unique<IEventHandle>(recipient, ctx.SelfID, ev, IEventHandle::MakeFlags(channel, 0), cookie);
    if (TEvVResultBase *base = dynamic_cast<TEvVResultBase *>(ev)) {
        base->FinalizeAndSend(ctx, std::move(event));
    } else {
        TActivationContext::Send(event.release());
    }
}

}//NKikimr
