#include "vdisk_response.h"
#include "vdisk_events.h"
#include <ydb/core/base/interconnect_channels.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <util/system/datetime.h>

namespace NKikimr {

template <class TRecord>
void ReportResponse(const TRecord& record, const TIntrusivePtr<TVDiskContext>& vCtx);
void LogOOSStatus(ui32 flags, const TLogoBlobID& blobId, const TString& vDiskLogPrefix, std::atomic<ui32>& curFlags);
void UpdateMonOOSStatus(ui32 flags, const std::shared_ptr<NMonGroup::TOutOfSpaceGroup>& monGroup);
void UpdateMonResponseStatus(NKikimrProto::EReplyStatus status, HandleClassType handleClass, const std::shared_ptr<NMonGroup::TResponseStatusGroup>& monGroup);

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, const TIntrusivePtr<TVDiskContext>& vCtx) {
    ui32 channel = TInterconnectChannels::IC_BLOBSTORAGE;
    if (TEvVResultBase *base = dynamic_cast<TEvVResultBase *>(ev)) {
        channel = base->GetChannelToSend();
    }
    SendVDiskResponse(ctx, recipient, ev, cookie, channel, vCtx);
}

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, ui32 channel, const TIntrusivePtr<TVDiskContext>& vCtx) {
    if (vCtx) {
        switch (ev->Type()) {
#define HANDLE_EVENT(T)                              \
            case T::EventType: {                     \
                T *event = static_cast<T *>(ev);     \
                ReportResponse(event->Record, vCtx); \
                break;                               \
            }
                
                HANDLE_EVENT(TEvBlobStorage::TEvVPutResult)
                HANDLE_EVENT(TEvBlobStorage::TEvVMultiPutResult)
                HANDLE_EVENT(TEvBlobStorage::TEvVGetResult)
                HANDLE_EVENT(TEvBlobStorage::TEvVGetBlockResult)
                HANDLE_EVENT(TEvBlobStorage::TEvVCollectGarbageResult)
#undef HANDLE_EVENT
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

template <typename T, typename Tuple>
struct IsInTypes;

template <typename T, typename... Types>
struct IsInTypes<T, std::tuple<Types...>> {
    static constexpr bool value = (std::is_same_v<T, Types> || ...);
};

struct TReportingOSStatus {
    using EnableFor = std::tuple<
        NKikimrBlobStorage::TEvVPutResult,
        NKikimrBlobStorage::TEvVMultiPutResult>;

    template <typename TRecord>
    static void Report(const TRecord& record, const TIntrusivePtr<TVDiskContext>& vCtx) {
        LogOOSStatus(record.GetStatusFlags(), LogoBlobIDFromLogoBlobID(record.GetBlobID()), vCtx->VDiskLogPrefix, vCtx->CurrentOOSStatusFlag);
        UpdateMonOOSStatus(record.GetStatusFlags(), vCtx->OOSMonGroup);
    }

    template<>
    void Report(const NKikimrBlobStorage::TEvVMultiPutResult& record, const TIntrusivePtr<TVDiskContext>& vCtx) {
        if (record.ItemsSize() > 0) {
            const auto& item = record.GetItems(0);
            LogOOSStatus(record.GetStatusFlags(), LogoBlobIDFromLogoBlobID(item.GetBlobID()), vCtx->VDiskLogPrefix, vCtx->CurrentOOSStatusFlag);
            UpdateMonOOSStatus(record.GetStatusFlags(), vCtx->OOSMonGroup);
        }
    }
};

struct TReportingResponseStatus {
    using EnableFor = std::tuple<
        NKikimrBlobStorage::TEvVPutResult,
        NKikimrBlobStorage::TEvVMultiPutResult,
        NKikimrBlobStorage::TEvVGetResult,
        NKikimrBlobStorage::TEvVGetBlockResult,
        NKikimrBlobStorage::TEvVCollectGarbageResult>;

    template <typename TRecord>
    static void Report(const TRecord& record, const TIntrusivePtr<TVDiskContext>& vCtx) {
        UpdateMonResponseStatus(record.GetStatus(), record.GetHandleClass(), vCtx->ResponseStatusMonGroup);
    }

    template<>
    void Report(const NKikimrBlobStorage::TEvVMultiPutResult& record, const TIntrusivePtr<TVDiskContext>& vCtx) {
    for (const auto& item : record.GetItems()) {
        UpdateMonResponseStatus(item.GetStatus(), record.GetHandleClass(), vCtx->ResponseStatusMonGroup);
    }
}
};

#define DEFUNE_REPORT(NAME)                                                                              \
    template <typename TRecord>                                                                          \
    typename std::enable_if<IsInTypes<TRecord, TReporting##NAME::EnableFor>::value>::type Report##NAME(  \
            const TRecord& record, const TIntrusivePtr<TVDiskContext>& vCtx) {                           \
        TReporting##NAME::Report(record, vCtx);                                                          \
    }                                                                                                    \
                                                                                                         \
    template <typename TRecord>                                                                          \
    typename std::enable_if<!IsInTypes<TRecord, TReporting##NAME::EnableFor>::value>::type Report##NAME( \
            const TRecord& record, const TIntrusivePtr<TVDiskContext>& vCtx) {}

    DEFUNE_REPORT(OSStatus)
    DEFUNE_REPORT(ResponseStatus)
#undef DEFUNE_REPORT

template <class TRecord>
void ReportResponse(const TRecord& record, const TIntrusivePtr<TVDiskContext>& vCtx) {
    ReportOSStatus(record, vCtx);
    ReportResponseStatus(record, vCtx);
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

void UpdateMonResponseStatus(NKikimrProto::EReplyStatus status, HandleClassType handleClass,
        const std::shared_ptr<NMonGroup::TResponseStatusGroup>& monGroup) {
    if (!monGroup) {
        return;
    }

    monGroup->GetCounter(status, handleClass).Inc();
}

} //NKikimr
