#pragma once
#include "defs.h"

#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_requestimpl.h"
#include "blobstorage_pdisk_util_signal_event.h"

#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Completion actions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TRequestBase;

class TPDisk;

class TCompletionEventSender : public TCompletionAction {
    TPDisk *PDisk;
    const TActorId Recipient;
    THolder<IEventBase> Event;
    ::NMonitoring::TDynamicCounters::TCounterPtr Counter;

public:
    THolder<TRequestBase> Req;

    TCompletionEventSender(TPDisk *pDisk, const TActorId &recipient, IEventBase *event,
            ::NMonitoring::TDynamicCounters::TCounterPtr &counter)
        : PDisk(pDisk)
        , Recipient(recipient)
        , Event(event)
        , Counter(counter)
    {}

    TCompletionEventSender(TPDisk *pDisk, const TActorId &recipient, IEventBase *event)
        : PDisk(pDisk)
        , Recipient(recipient)
        , Event(event)
        , Counter(nullptr)
    {}

    TCompletionEventSender(TPDisk *pDisk, THolder<TRequestBase> req)
        : PDisk(pDisk)
        , Counter(nullptr)
        , Req(std::move(req))
    {}

    TCompletionEventSender(TPDisk *pDisk)
        : PDisk(pDisk)
        , Counter(nullptr)
    {}

    void Exec(TActorSystem *actorSystem) override;

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        delete this;
    }
};

class TCompletionChunkWrite : public TCompletionAction {
    const TActorId Recipient;
    THolder<TEvChunkWriteResult> Event;
    TPDiskMon *Mon;
    ui32 PDiskId;
    NHPTimer::STime StartTime;
    size_t SizeBytes;
    ui8 PriorityClass;
    std::function<void()> OnDestroy;
    TReqId ReqId;
    NWilson::TSpan Span;

public:
    TEvChunkWrite::TPartsPtr Parts;
    std::optional<TAlignedData> Buffer;

    TCompletionChunkWrite(const TActorId &recipient, TEvChunkWriteResult *event,
            TPDiskMon *mon, ui32 pdiskId, NHPTimer::STime startTime, size_t sizeBytes,
            ui8 priorityClass, std::function<void()> onDestroy, TReqId reqId,
            NWilson::TSpan&& span)
        : Recipient(recipient)
        , Event(event)
        , Mon(mon)
        , PDiskId(pdiskId)
        , StartTime(startTime)
        , SizeBytes(sizeBytes)
        , PriorityClass(priorityClass)
        , OnDestroy(std::move(onDestroy))
        , ReqId(reqId)
        , Span(std::move(span))
    {
        TCompletionAction::ShouldBeExecutedInCompletionThread = false;
    }

    const void *GetBuffer() {
        if (Buffer) {
            return Buffer->Get();
        } else if (Parts->Size() == 1) {
            return (*Parts)[0].first;
        } else {
            return nullptr;
        }
    }

    size_t CompactBuffer(size_t tailroom, size_t sectorSize) {
        size_t totalSize = 0;
        for (size_t i = 0; i < Parts->Size(); ++i) {
            totalSize += (*Parts)[i].second;
        }
        totalSize += tailroom;

        totalSize = AlignUp<ui64>(totalSize, sectorSize);
        Buffer.emplace(totalSize);

        size_t written = 0;

        // body
        for (size_t i = 0; i < Parts->Size(); ++i) {
            auto [ptr, size] = (*Parts)[i];
            if (ptr) {
                memcpy(Buffer->Get() + written, ptr, size);
            } else {
                memset(Buffer->Get() + written, 0, size);
            }
            written += size;
        }

        // tail
        if (written < totalSize) {
            auto size = totalSize - written;
            memset(Buffer->Get() + written, 0, size);
            written += size;
        }
        Y_VERIFY(written == totalSize);
        return totalSize;
    }

    ~TCompletionChunkWrite() {
        OnDestroy();
    }

    void Exec(TActorSystem *actorSystem) override {
        auto execSpan = Span.CreateChild(TWilson::PDiskDetailed, "PDisk.CompletionChunkWrite.Exec");
        double responseTimeMs = HPMilliSecondsFloat(HPNow() - StartTime);
        STLOGX(*actorSystem, PRI_DEBUG, BS_PDISK, BPD01, "TCompletionChunkWrite::Exec",
                (DiskId, PDiskId),
                (ReqId, ReqId),
                (Event, Event->ToString()),
                (PriorityClass, (ui32)PriorityClass),
                (timeMs, responseTimeMs),
                (sizeBytes, SizeBytes));
        if (Mon) {
            Mon->IncrementResponseTime(PriorityClass, responseTimeMs, SizeBytes);
        }
        LWTRACK(PDiskChunkResponseTime, Orbit, PDiskId, ReqId.Id, PriorityClass, responseTimeMs, SizeBytes);
        Event->Orbit = std::move(Orbit);
        actorSystem->Send(Recipient, Event.Release());
        if (Mon) {
            Mon->GetWriteCounter(PriorityClass)->CountResponse();
        }
        execSpan.EndOk();
        Span.EndOk();
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        Event->Status = NKikimrProto::CORRUPTED;
        Event->ErrorReason = ErrorReason;
        actorSystem->Send(Recipient, Event.Release());
        Span.EndError(ErrorReason);
        delete this;
    }
};

class TCompletionLogWrite : public TCompletionAction {
    TPDisk *PDisk;
    TVector<TLogWrite*> LogWriteQueue;
    TVector<TLogWrite*> Commits;
    TVector<ui32> CommitedLogChunks;
public:
    TCompletionLogWrite(TPDisk *pDisk, TVector<TLogWrite*>&& logWriteQueue, TVector<TLogWrite*>&& commits,
            TVector<ui32>&& commitedLogChunks)
        : PDisk(pDisk)
        , LogWriteQueue(std::move(logWriteQueue))
        , Commits(std::move(commits))
        , CommitedLogChunks(std::move(commitedLogChunks))
    {
        TCompletionAction::ShouldBeExecutedInCompletionThread = false;
    }

    TVector<ui32>* GetCommitedLogChunksPtr() {
        return &CommitedLogChunks;
    }

    void Exec(TActorSystem *actorSystem) override;
    void Release(TActorSystem *actorSystem) override;

    virtual ~TCompletionLogWrite() {
        for (auto it = LogWriteQueue.begin(); it != LogWriteQueue.end(); ++it) {
            delete *it;
        }
    }
};

class TCompletionChunkRead : public TCompletionAction {
    static constexpr ui64 ReferenceCanary = 6422729157296672589ull;

    TPDisk *PDisk;
    TIntrusivePtr<TChunkRead> Read;
    TBufferWithGaps CommonBuffer;
    TMutex CommonBufferMutex; // used to protect CommonBuffer when gaps are being add
    TAtomic PartsPending;
    TAtomic Deletes;
    std::function<void()> OnDestroy;
    ui64 ChunkNonce;
    NWilson::TSpan Span;

    const ui64 DoubleFreeCanary;
public:
    TCompletionChunkRead(TPDisk *pDisk, TIntrusivePtr<TChunkRead> &read, std::function<void()> onDestroy,
            ui64 chunkNonce, IRcBufAllocator* alloc, NWilson::TSpan&& span);
    void Exec(TActorSystem *actorSystem) override;
    ~TCompletionChunkRead();
    void ReplyError(TActorSystem *actorSystem, TString reason);
    // Returns true if there is some pending requests to wait
    bool PartReadComplete(TActorSystem *actorSystem);

    void AddPart() {
        AtomicIncrement(PartsPending);
    }

    TBufferWithGaps *GetCommonBuffer() {
        return &CommonBuffer;
    }

    void AddGap(ui32 start, ui32 end) {
        TGuard<TMutex> g(CommonBufferMutex);
        CommonBuffer.AddGap(start, end);
    }

    ui64 GetChunkNonce() {
        return ChunkNonce;
    }

    bool PartDeleted(TActorSystem *actorSystem) {
        AtomicIncrement(Deletes);
        return PartReadComplete(actorSystem);
    }

    void Release(TActorSystem *actorSystem) override {
        ReplyError(actorSystem, "TCompletionChunkRead is released");
        Span.EndError("release");
    }
};

class TCompletionChunkReadPart : public TCompletionAction {
    TPDisk *PDisk;
    TIntrusivePtr<TChunkRead> Read;
    ui32 RawReadSize;
    ui64 PayloadReadSize;
    ui64 CommonBufferOffset;
    TCompletionChunkRead *CumulativeCompletion;
    ui64 ChunkNonce;
    ui8 *Destination = nullptr;
    TBuffer::TPtr Buffer;
    bool IsTheLastPart;
    NWilson::TSpan Span;
public:
    TCompletionChunkReadPart(TPDisk *pDisk, TIntrusivePtr<TChunkRead> &read, ui64 rawReadSize, ui64 payloadReadSize,
            ui64 commonBufferOffset, TCompletionChunkRead *cumulativeCompletion, bool isTheLastPart,
            NWilson::TSpan&& span);


    bool CanHandleResult() const override {
        return true;
    }

    TBuffer *GetBuffer();
    void UnencryptData(TActorSystem *actorSystem);

    void Exec(TActorSystem *actorSystem) override;
    void Release(TActorSystem *actorSystem) override;
    virtual ~TCompletionChunkReadPart();
};

static_assert(sizeof(TAtomicBase) >= sizeof(void *), "There is a problem with CompletionActionPtr!");

class TCumulativeCompletionHolder {
    TAtomic PartsPending;
    TAtomic Releases;
    TAtomic CompletionActionPtr;
public:
    TCumulativeCompletionHolder()
        : PartsPending(0)
        , Releases(0)
        , CompletionActionPtr((TAtomicBase)nullptr)
    {}

    void SetCompletionAction(TCompletionAction *completionAction) {
        AtomicSet(CompletionActionPtr, (TAtomicBase)completionAction);
        Y_VERIFY(AtomicGet(PartsPending) > 0);
    }

    void Ref() {
        AtomicIncrement(PartsPending);
    }

    void Release(TActorSystem *actorSystem) {
        AtomicIncrement(Releases);
        Exec(actorSystem);
    }

    void Exec(TActorSystem *actorSystem) {
        TAtomicBase partsPending = AtomicDecrement(PartsPending);
        if (partsPending == 0) {
            TCompletionAction *completionAction = (TCompletionAction*)AtomicGet(CompletionActionPtr);
            if (completionAction) {
                if (AtomicGet(Releases)) {
                    completionAction->Release(actorSystem);
                } else {
                    completionAction->Exec(actorSystem);
                }
            }
            delete this;
        }
    }
};

class TCompletionPart : public TCompletionAction {
    TCumulativeCompletionHolder *CumulativeCompletionHolder;
public:
    TCompletionPart(TCumulativeCompletionHolder *cumulativeCompletionHolder)
        : CumulativeCompletionHolder(cumulativeCompletionHolder)
    {
        cumulativeCompletionHolder->Ref();
    }

    void Exec(TActorSystem *actorSystem) override {
        CumulativeCompletionHolder->Exec(actorSystem);
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        CumulativeCompletionHolder->Release(actorSystem);
        delete this;
    }
};

class TCompletionSignal : public TCompletionAction {
    TSignalEvent *Event;

public:
    TCompletionSignal(TSignalEvent *event)
        : Event(event)
    {}

    void Exec(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        Event->Signal();
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        Exec(actorSystem);
    }
};

class TChunkTrimCompletion : public TCompletionAction {
    TPDisk *PDisk;
    NHPTimer::STime StartTime;
    size_t SizeBytes;
    TReqId ReqId;

public:
    TChunkTrimCompletion(TPDisk *pdisk, NHPTimer::STime startTime, size_t sizeBytes, TReqId reqId)
        : PDisk(pdisk)
        , StartTime(startTime)
        , SizeBytes(sizeBytes)
        , ReqId(reqId)
    {}

    void Exec(TActorSystem *actorSystem) override;

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        delete this;
    }
};

class TChunkShredCompletion : public TCompletionAction {
    TPDisk *PDisk;
    TChunkIdx Chunk;
    ui32 SectorIdx;
    size_t SizeBytes;
    TReqId ReqId;

public:
    TChunkShredCompletion(TPDisk *pdisk, TChunkIdx chunk, ui32 sectorIdx, size_t sizeBytes, TReqId reqId)
        : PDisk(pdisk)
        , Chunk(chunk)
        , SectorIdx(sectorIdx)
        , SizeBytes(sizeBytes)
        , ReqId(reqId)
    {}

    void Exec(TActorSystem *actorSystem) override;

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        delete this;
    }
};

class TCompletionSequence : public TCompletionAction {
    TVector<TCompletionAction*> Actions;

public:
    TCompletionSequence() = default;
    TCompletionSequence(const TVector<TCompletionAction*>& actions)
        : Actions(actions)
    {}

    TCompletionSequence(TVector<TCompletionAction*>&& actions)
        : Actions(std::move(actions))
    {}

    TCompletionSequence& operator=(const TCompletionSequence&) = delete;
    TCompletionSequence& operator=(TCompletionSequence&&) = delete;

    void Exec(TActorSystem *actorSystem) override {
        for (TCompletionAction* action : Actions) {
            action->Exec(actorSystem);
        }
    }

    void Release(TActorSystem *actorSystem) override {
        for (TCompletionAction* action : Actions) {
            action->Release(actorSystem);
        }
    }
};

} // NPDisk
} // NKikimr
