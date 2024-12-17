#include "vdisk_response.h"
#include "vdisk_events.h"
#include <ydb/core/base/interconnect_channels.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <util/system/datetime.h>

namespace NKikimr {

template <class TRecord>
void ReportResponse(TRecord record, const TLogoBlobID& blobId, const TIntrusivePtr<TVDiskContext>& vCtx);

template <class TRecord>
void ReportOSStatus(TRecord record, const TLogoBlobID& blobId, const TIntrusivePtr<TVDiskContext>& vCtx);
void LogOOSStatus(ui32 flags, const TLogoBlobID& blobId, const TString& vDiskLogPrefix, std::atomic<ui32>& curFlags);
void UpdateMonOOSStatus(ui32 flags, const std::shared_ptr<NMonGroup::TOutOfSpaceGroup>& monGroup);

template <class TRecord>
void ReportResponseStatus(TRecord record, const TLogoBlobID& blobId, const TIntrusivePtr<TVDiskContext>& vCtx);
void UpdateMonResponseStatus(NKikimrProto::EReplyStatus status, const std::shared_ptr<NMonGroup::TResponseStatusGroup>& monGroup);

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, const TIntrusivePtr<TVDiskContext>& vCtx) {
    ui32 channel = TInterconnectChannels::IC_BLOBSTORAGE;
    if (TEvVResultBase *base = dynamic_cast<TEvVResultBase *>(ev)) {
        channel = base->GetChannelToSend();
    }
    SendVDiskResponse(ctx, recipient, ev, cookie, channel, vCtx);
}

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, ui32 channel, const TIntrusivePtr<TVDiskContext>& vCtx) {
    if (vCtx) {
        switch(ev->Type()) {
            case TEvBlobStorage::TEvVPutResult::EventType: {
                TEvBlobStorage::TEvVPutResult* event = static_cast<TEvBlobStorage::TEvVPutResult *>(ev);
                ReportResponse(event->Record, LogoBlobIDFromLogoBlobID(event->Record.GetBlobID()), vCtx);
                break;
            }
            case TEvBlobStorage::TEvVMultiPutResult::EventType: {
                TEvBlobStorage::TEvVMultiPutResult *event = static_cast<TEvBlobStorage::TEvVMultiPutResult *>(ev);
                if (event->Record.ItemsSize() > 0) {
                    const auto& item = event->Record.GetItems(0);
                    ReportResponse(event->Record, LogoBlobIDFromLogoBlobID(item.GetBlobID()), vCtx);
                }
                break;
            }
        }
    }

    switch (const ui32 type = ev->Type()) {
#define HANDLE_EVENT(T) \
        case TEvBlobStorage::T::EventType: { \
            TEvBlobStorage::T *event = static_cast<TEvBlobStorage::T *>(ev); \
            const double usPerCycle = 1000000.0 / NHPTimer::GetCyclesPerSecond(); \
            event->Record.MutableTimestamps()->SetSentByVDiskUs(GetCycleCountFast() * usPerCycle); \
            break; \
        }

        HANDLE_EVENT(TEvVPutResult)
        HANDLE_EVENT(TEvVMultiPutResult)
        HANDLE_EVENT(TEvVGetResult)
        HANDLE_EVENT(TEvVPatchFoundParts)
        HANDLE_EVENT(TEvVPatchXorDiffResult)
        HANDLE_EVENT(TEvVPatchResult)

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

template <class TRecord>
void ReportResponse(TRecord record, const TLogoBlobID& blobId, const TIntrusivePtr<TVDiskContext>& vCtx) {
    ReportOSStatus(record, blobId, vCtx);
    ReportResponseStatus(record, blobId, vCtx);
}

template <class TRecord>
void ReportResponseStatus(TRecord record, const TLogoBlobID& blobId, const TIntrusivePtr<TVDiskContext>& vCtx) {
   UpdateMonResponseStatus(record.GetStatus(), vCtx->ResponseStatusMonGroup);
}

void UpdateMonResponseStatus(NKikimrProto::EReplyStatus status, const std::shared_ptr<NMonGroup::TResponseStatusGroup>& monGroup) {
    if (!monGroup) {
        return;
    }

    if (status == NKikimrProto::ERROR) {
         monGroup->ResponsesWithStatusError().Inc();
    } else if (status == NKikimrProto::RACE) {
        monGroup->ResponsesWithStatusRace().Inc();
    } else if (status == NKikimrProto::BLOCKED) {
        monGroup->ResponsesWithStatusBlocked().Inc();
    } else if (status == NKikimrProto::OUT_OF_SPACE) {
        monGroup->ResponsesWithStatusOutOfSpace().Inc();
    } else if (status == NKikimrProto::DEADLINE) {
        monGroup->ResponsesWithStatusDeadline().Inc();
    } else if (status == NKikimrProto::NOTREADY) {
        monGroup->ResponsesWithStatusNotReady().Inc();
    } else if (status == NKikimrProto::VDISK_ERROR_STATE) {
        monGroup->ResponsesWithStatusVdiskErrorState().Inc();
    }
}

template <class TRecord>
void ReportOSStatus(TRecord record, const TLogoBlobID& blobId, const TIntrusivePtr<TVDiskContext>& vCtx) {
    LogOOSStatus(record.GetStatusFlags(), blobId, vCtx->VDiskLogPrefix, vCtx->CurrentOOSStatusFlag);
    UpdateMonOOSStatus(record.GetStatusFlags(), vCtx->OOSMonGroup);
}

void LogOOSStatus(ui32 flags, const TLogoBlobID& blobId, const TString& vDiskLogPrefix, std::atomic<ui32>& curFlags) {
    if (!TlsActivationContext) {
        return;
    }
    if (!(flags & NKikimrBlobStorage::StatusIsValid)) {
        return;
    }

    ui32 prevFlags = curFlags.exchange(flags, std::memory_order_relaxed);
    if (prevFlags == flags) {
        return;
    }

    LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_VDISK_CHUNKS,
        vDiskLogPrefix << "Disk space status changed to " <<
        TPDiskSpaceColor_Name(StatusFlagToSpaceColor(flags)) << " on blob " << blobId.ToString() << "; " <<
        "oldFlags: " << prevFlags << ", newFlags: " << flags);
}

void UpdateMonOOSStatus(ui32 flags, const std::shared_ptr<NMonGroup::TOutOfSpaceGroup>& monGroup) {
    if (!monGroup) {
        return;
    }
    if (!(flags & NKikimrBlobStorage::StatusIsValid)) {
        return;
    }

    if (flags & NKikimrBlobStorage::StatusDiskSpaceRed) {
        monGroup->ResponsesWithDiskSpaceRed().Inc();
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceOrange) {
        monGroup->ResponsesWithDiskSpaceOrange().Inc();
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceLightOrange) {
        monGroup->ResponsesWithDiskSpaceLightOrange().Inc();
    } else if (flags & NKikimrBlobStorage::StatusDiskSpacePreOrange) {
        monGroup->ResponsesWithDiskSpacePreOrange().Inc();
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceYellowStop) {
        monGroup->ResponsesWithDiskSpaceYellowStop().Inc();
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceLightYellowMove) {
        monGroup->ResponsesWithDiskSpaceLightYellowMove().Inc();
    }
}

} //NKikimr
