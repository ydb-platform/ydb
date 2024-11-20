#pragma once

#include "defs.h"
#include "common.h"

namespace NKikimr::NBsQueue {

template<typename T> struct TMatchingResultType;
template<> struct TMatchingResultType<TEvBlobStorage::TEvVMovedPatch> { using Type = TEvBlobStorage::TEvVMovedPatchResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVPatchStart> { using Type = TEvBlobStorage::TEvVPatchFoundParts; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVPatchDiff> { using Type = TEvBlobStorage::TEvVPatchResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVPatchXorDiff> { using Type = TEvBlobStorage::TEvVPatchXorDiffResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVPut> { using Type = TEvBlobStorage::TEvVPutResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVMultiPut> { using Type = TEvBlobStorage::TEvVMultiPutResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVGet> { using Type = TEvBlobStorage::TEvVGetResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVBlock> { using Type = TEvBlobStorage::TEvVBlockResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVGetBlock> { using Type = TEvBlobStorage::TEvVGetBlockResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVCollectGarbage> { using Type = TEvBlobStorage::TEvVCollectGarbageResult; };
template<> struct TMatchingResultType<TEvBlobStorage::TEvVGetBarrier> { using Type = TEvBlobStorage::TEvVGetBarrierResult; };
template<typename T> using TMatchingResultTypeT = typename TMatchingResultType<T>::Type;

template<typename TPtr>
inline NLWTrace::TOrbit MoveOrbit(TPtr&) {
    return {};
}

template<>
inline NLWTrace::TOrbit MoveOrbit<TEvBlobStorage::TEvVPut::TPtr>(TEvBlobStorage::TEvVPut::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get());
    return std::move(ev->Get()->Orbit);
}

class TEventHolder {
    ui32 Type;
    ui32 ByteSize;
    TActorId Sender;
    ui64 Cookie;
    ui32 InterconnectChannel;
    mutable NLWTrace::TOrbit Orbit;
    TIntrusivePtr<TEventSerializedData> Buffer;
    TBSProxyContextPtr BSProxyCtx;
    std::unique_ptr<IEventBase> LocalEvent;
    std::optional<std::weak_ptr<TMessageRelevanceTracker>> Tracker;

public:
    TEventHolder()
        : Type(0)
        , ByteSize(0)
        , Cookie(0)
        , InterconnectChannel(0)
    {}

    template<typename TPtr>
    TEventHolder(TPtr& ev, const ::NMonitoring::TDynamicCounters::TCounterPtr& serItems,
            const ::NMonitoring::TDynamicCounters::TCounterPtr& serBytes, const TBSProxyContextPtr& bspctx,
            ui32 interconnectChannel, bool local)
        : Type(ev->GetTypeRewrite())
        , Sender(ev->Sender)
        , Cookie(ev->Cookie)
        , InterconnectChannel(interconnectChannel)
        , Orbit(MoveOrbit(ev))
        , BSProxyCtx(bspctx)
        , Tracker(std::move(ev->Get()->MessageRelevanceTracker))
    {
        // trace the event
        if constexpr (std::is_same_v<TPtr, TEvBlobStorage::TEvVPut::TPtr>) {
            const auto& record = ev->Get()->Record;
            TLogoBlobID blob = LogoBlobIDFromLogoBlobID(record.GetBlobID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
            LWTRACK(DSQueueVPutIsQueued, Orbit, vDiskId.GroupID.GetRawId(), blob.ToString(), blob.Channel(), blob.PartId(),
                    blob.BlobSize());
        }

        if (local && ev->HasEvent()) {
            ByteSize = ev->Get()->GetCachedByteSize();
            LocalEvent.reset(ev->ReleaseBase().Release());
        } else {
            const bool hasEvent = ev->HasEvent();
            Buffer = ev->ReleaseChainBuffer();
            ByteSize = Buffer->GetSize();
            if (hasEvent) {
                ++*serItems;
                *serBytes += ByteSize;
            }
        }

        BSProxyCtx->Queue.Add(ByteSize);
        ev.Reset();
    }

    ~TEventHolder() {
        Discard();
    }

    bool Relevant() const {
        return !Tracker || !Tracker->expired();
    }

    ui32 GetByteSize() const {
        return ByteSize;
    }

    template<typename TCallable>
    auto Apply(TCallable&& callable) {
        switch (Type) {
#define CASE(T) case TEvBlobStorage::T::EventType: return callable(static_cast<TEvBlobStorage::T*>(LocalEvent.get()))
            CASE(TEvVMovedPatch);
            CASE(TEvVPatchStart);
            CASE(TEvVPatchDiff);
            CASE(TEvVPatchXorDiff);
            CASE(TEvVPut);
            CASE(TEvVMultiPut);
            CASE(TEvVGet);
            CASE(TEvVBlock);
            CASE(TEvVGetBlock);
            CASE(TEvVCollectGarbage);
            CASE(TEvVGetBarrier);
            default: Y_ABORT();
#undef CASE
        }
    }

    IEventBase *MakeErrorReply(NKikimrProto::EReplyStatus status, const TString& errorReason,
            const ::NMonitoring::TDynamicCounters::TCounterPtr& deserItems,
            const ::NMonitoring::TDynamicCounters::TCounterPtr& deserBytes);

    ui32 GetType() const {
        return Type;
    }

    const TActorId& GetSender() const {
        return Sender;
    }

    ui64 GetCookie() const {
        return Cookie;
    }

    NLWTrace::TOrbit& GetOrbit() const {
        return Orbit;
    }

    void SendToVDisk(const TActorContext& ctx, const TActorId& remoteVDisk, ui64 queueCookie, ui64 msgId, ui64 sequenceId,
            bool sendMeCostSettings, NWilson::TTraceId traceId, const NBackpressure::TQueueClientId& clientId,
            const TBSQueueTimer& processingTimer);

    void Discard();
};

} // NKikimr::NBsQueue
