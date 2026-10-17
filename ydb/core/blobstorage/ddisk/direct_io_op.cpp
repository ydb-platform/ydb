#include "ddisk_actor.h"
#include "ddisk_checksums.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/overloaded.h>
#include <util/stream/format.h>

#include <algorithm>
#include <cerrno>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_DDISK

namespace NKikimr::NDDisk {

static constexpr size_t MaxRwCount = 0x7ffff000ULL; // INT_MAX & PAGE_MASK on 4K pages, ~ 2 GiB
static constexpr size_t MinBlockSize = 4096;

using TReplyStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

namespace {

// a poor error mapping (we can't map io_uring errors 1:1 to our errors)
TReplyStatus::E UringErrorToStatus(i64 result, NPDisk::TUringOperationBase::EOperationType opType) {
    const int err = static_cast<int>(-result);
    switch (err) {
        case EAGAIN:
#if EAGAIN != EWOULDBLOCK
        case EWOULDBLOCK:
#endif
        case ENOSPC:
        case ENOMEM:
            return TReplyStatus::OVERLOADED;
        case EINVAL:
            return TReplyStatus::INCORRECT_REQUEST;
        case EIO:
            return opType == NPDisk::TUringOperationBase::EREAD
                ? TReplyStatus::LOST_DATA
                : TReplyStatus::ERROR;
        default:
            return TReplyStatus::ERROR;
    }
}

} // anonymous

// Keep the actor reference independently of the operation: recycling or publishing
// a retry can immediately transfer the operation to another thread.
class TDDiskActor::TDirectIoOpBase::TCompletionGuard {
    TDDiskActor& Actor;
    NActors::TActorSystem* const ActorSystem;
    TDirectIoOpBase* Op;

public:
    TCompletionGuard(TDirectIoOpBase* op, NActors::TActorSystem* actorSystem)
        : Actor(op->Actor)
        , ActorSystem(actorSystem)
        , Op(op)
    {}

    ~TCompletionGuard() {
        if (Op) {
            Op->SelfRecycle();
        }
        Actor.OnDirectIODone(ActorSystem);
    }

    std::unique_ptr<TDirectIoOpBase> Release() {
        return std::unique_ptr<TDirectIoOpBase>(std::exchange(Op, nullptr));
    }

    TCompletionGuard(const TCompletionGuard&) = delete;
    TCompletionGuard& operator=(const TCompletionGuard&) = delete;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDirectIoOpBase
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TDDiskActor::TDirectIoOpBase::TDirectIoOpBase(TDDiskActor& actor)
    : Actor(actor)
    , DDiskId(actor.SelfId())
    , StartTs(HPNow())
{}

TDDiskActor::TDirectIoOpBase::~TDirectIoOpBase() = default;

TDDiskActor::TOpCountersBase& TDDiskActor::TDirectIoOpBase::GetCounters() const {
    switch (GetOperationType()) {
    case TUringOperationBase::EREAD:
        return Actor.Counters.DirectIO.Read;
    case TUringOperationBase::EWRITE:
        return Actor.Counters.DirectIO.Write;
    default:
        Y_ABORT("Unknown OperationType");
    }
}

void TDDiskActor::TDirectIoOpBase::OnComplete(NActors::TActorSystem* actorSystem) noexcept {
    TCompletionGuard guard(this, actorSystem);

    const auto opType = GetOperationType();
    const i64 result = GetResult();
    const double requestTimeMs = TimePassed();
    AccountShortIo();

    // Critical I/O overload retries retain the operation and all its buffers.
    // Defer logical Done() until retries finish or fail terminally.
    if (Y_UNLIKELY(PrepareRetry())) {
        ++RetryCount;
        auto ev = std::make_unique<TDDiskActor::TEvPrivate::TEvRetryIO>(guard.Release());
        actorSystem->Send(new IEventHandle(DDiskId, {}, ev.release()));
        return;
    }

    GetCounters().Done(GetTotalSize(), requestTimeMs);

    if (Y_UNLIKELY(result < 0)) {
        const char* opName = opType == TUringOperationBase::EREAD ? "read" : "write";
        const auto bufAddr = reinterpret_cast<uintptr_t>(GetIovBase());
        TString reason = TStringBuilder()
            << "io_uring " << opName << " error:"
            << " errno=" << (-result) << " (" << strerror(-result) << ")"
            << " diskOffset=" << GetDiskOffset()
            << " totalSize=" << GetTotalSize()
            << " iovLen=" << GetOperationBytes()
            << " bufAddr=0x" << Hex(bufAddr)
            << " bufAligned4k=" << (int)(bufAddr % MinBlockSize == 0)
            << " offsetAligned4k=" << (int)(GetDiskOffset() % MinBlockSize == 0)
            << " sizeAligned4k=" << (int)(GetOperationBytes() % MinBlockSize == 0)
            << " chunkIdx=" << ChunkIdx
            << " chunkOffset=" << ChunkOffsetInBytes
            << " DDiskId=" << DDiskId;
        YDB_LOG_ERROR_CTX(*actorSystem, reason);
        const bool exhausted = IsCriticalDDiskIo()
            && UringErrorToStatus(result, opType) == TReplyStatus::OVERLOADED;
        if (exhausted) {
            reason += TStringBuilder() << " retry exhausted: attempts=" << (RetryCount + 1);
        }
        Reply(actorSystem, exhausted ? TReplyStatus::ERROR : UringErrorToStatus(result, opType), std::move(reason));
        return;
    }

    // Both the router and the PDisk fallback complete the whole logical request.
    Y_ABORT_UNLESS(static_cast<ui64>(result) == GetTotalSize());
    Reply(actorSystem, TReplyStatus::OK);
}

bool TDDiskActor::TDirectIoOpBase::PrepareRetry() noexcept {
    return GetResult() < 0 && IsCriticalDDiskIo()
        && UringErrorToStatus(GetResult(), GetOperationType()) == TReplyStatus::OVERLOADED
        && RetryCount < MaxResubmissions;
}

void TDDiskActor::TDirectIoOpBase::OnDrop(NActors::TActorSystem* actorSystem) noexcept {
    TCompletionGuard guard(this, actorSystem);
    AccountShortIo();

    GetCounters().Done(GetTotalSize());

    Reply(actorSystem, TReplyStatus::SESSION_MISMATCH, "io_uring request dropped");
}

void TDDiskActor::TDirectIoOpBase::AccountShortIo() noexcept {
    const ui64 count = TakeShortIoCount();
    if (!count) {
        return;
    }
    switch (GetOperationType()) {
    case TUringOperationBase::EREAD:
        *Actor.Counters.DirectIO.ShortReads += count;
        break;
    case TUringOperationBase::EWRITE:
        *Actor.Counters.DirectIO.ShortWrites += count;
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }
}

void TDDiskActor::TDirectIoOpBase::PrepareWrite(TRope&& data, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset) {
    Y_ABORT_UNLESS(data.size() <= MaxRwCount);
    const size_t dataSize = data.size();
    Data.reset();
    AlignedDataHolder = {};

    SetOperationType(EWRITE);

#if defined(__linux__)
    // Zero-copy scatter-gather path: taken when all rope chunks are page-aligned
    // (base address) and sector-aligned (length), and fit within MAX_IOVS. The
    // rope is moved into Data so its chunk backends (reference-counted heap
    // buffers) outlive the I/O; each chunk becomes one iovec - no memcpy.
    {
        size_t chunkCount = 0;
        bool allAligned = true;
        for (auto it = data.Begin(); it.Valid(); it.AdvanceToNextContiguousBlock()) {
            const uintptr_t base = reinterpret_cast<uintptr_t>(it.ContiguousData());
            if ((base & (MinBlockSize - 1)) != 0 || (it.ContiguousSize() & (MinBlockSize - 1)) != 0) {
                allAligned = false;
                break;
            }
            ++chunkCount;
            if (chunkCount > NPDisk::TUringOperationBase::MAX_IOVS) {
                allAligned = false;
                break;
            }
        }

        if (allAligned && chunkCount > 0) {
            Data = std::move(data);

            PrepareScatterGather(chunkCount, offset);
            for (auto it = Data->Begin(); it.Valid(); it.AdvanceToNextContiguousBlock()) {
                // writev only reads from the buffer, so const_cast is safe here.
                AddIov(const_cast<char*>(it.ContiguousData()), it.ContiguousSize());
            }

            ChunkIdx = chunkIdx;
            ChunkOffsetInBytes = chunkOffset;
            return;
        }
    }
#endif

    // Copy path: unaligned chunks, too many chunks, or non-Linux.
    AlignedDataHolder = TRcBuf::UninitializedPageAligned(dataSize);
    data.Begin().ExtractPlainDataAndAdvance(AlignedDataHolder.GetDataMut(), dataSize);

    // UnsafeGetDataMut: writev only reads from the buffer, so we avoid COW
    // that TRcBuf::GetDataMut() would trigger on shared page-aligned buffers.
    PrepareIov(AlignedDataHolder.UnsafeGetDataMut(), dataSize, offset);

    ChunkIdx = chunkIdx;
    ChunkOffsetInBytes = chunkOffset;
}

void TDDiskActor::TDirectIoOpBase::PrepareRead(size_t size, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset) {
    Y_ABORT_UNLESS(size <= MaxRwCount);
    Data.reset();

    AlignedDataHolder = TRcBuf::UninitializedPageAligned(size);
    SetOperationType(EREAD);
    PrepareIov(AlignedDataHolder.GetDataMut(), size, offset);

    ChunkIdx = chunkIdx;
    ChunkOffsetInBytes = chunkOffset;
}

TReadPayload TDDiskActor::TDirectIoOpBase::ExtractReadPayload() {
    if (Data) {
        return TReadPayload(std::move(*Data));
    }
    return TReadPayload(std::move(AlignedDataHolder));
}

TRope TDDiskActor::TDirectIoOpBase::ExtractData() {
    if (Data) {
        return std::move(*Data);
    }

    return TRope(std::move(AlignedDataHolder));
}



double TDDiskActor::TDirectIoOpBase::TimePassed() const {
    return HPMilliSecondsFloat(HPNow() - StartTs);
}

void TDDiskActor::TDirectIoOpBase::SetResult(i64 result, TRope&& data) {
    SetResult(result);
    Data = std::move(data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDDiskIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TDDiskIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status,
        TString reason) noexcept
{
    TIoCompletion completion;
    completion.Status = status;
    completion.ErrorMessage = std::move(reason);

    switch (GetOperationType()) {
    case TUringOperationBase::EREAD:
        if (status == TReplyStatus::OK) {
            completion.Data = ExtractReadPayload();
        }
        break;
    case TUringOperationBase::EWRITE:
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }

    // This operation owns the callback until recycling, independently of the frame.
    Y_UNUSED(actorSystem);
    Y_ABORT_UNLESS(Callback);
    Callback->OnComplete(std::move(completion), MetadataIndex);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TPersistentBufferPartIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TPersistentBufferPartIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status,
        TString reason) noexcept {
    std::unique_ptr<IEventBase> reply;
    const auto opType = GetOperationType();
    const i64 result = GetResult();
    if (status == TReplyStatus::OVERLOADED) {
        if (!reason) {
            reason = "io_uring request temporarily overloaded (I/O error retry)";
        }
    } else if (status != TReplyStatus::OK) {
        if (!reason) {
            if (result < 0) {
                const char* opName = opType == TUringOperationBase::EREAD
                    ? "read"
                    : (opType == TUringOperationBase::EWRITE ? "write" : "unknown");
                reason = TStringBuilder()
                    << opName
                    << " failed: " << strerror(-result)
                    << " (errno " << (-result) << ")";
            } else {
                reason = "I/O failed";
            }
        }
    }

    switch (opType) {
        case TUringOperationBase::EREAD: {
            TRope data = status == TReplyStatus::OK ? ExtractData() : TRope();
            reply = std::make_unique<TEvPrivate::TEvReadPersistentBufferPart>(
                GetCookie(), PartCookie, status, std::move(reason), std::move(data), IsRestore);
            break;
        }
        case TUringOperationBase::EWRITE:
            reply = std::make_unique<TEvPrivate::TEvWritePersistentBufferPart>(
                GetCookie(), PartCookie, status, reason, IsErase);
            break;
        default:
            Y_ABORT("Unknown OperationType");
    }

    actorSystem->Send(DDiskId, reply.release());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDirectIoOpBase — pool support
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TDirectIoOpBase::Reinit() {
    ResetSubmissionState();
    StartTs = HPNow();
    Cookie = 0;
    ChunkIdx = 0;
    ChunkOffsetInBytes = 0;
    RetryCount = 0;
}

void TDDiskActor::TDirectIoOpBase::ClearForRecycle() noexcept {
    AlignedDataHolder = {};
    Data.reset();
    RetryCount = 0;
}

void TDDiskActor::TDDiskIoOp::SelfRecycle() noexcept {
    Actor.ReturnOp(this);
}

void TDDiskActor::TDDiskIoOp::ClearForRecycle() noexcept {
    Callback.reset();
    MetadataIndex.reset();
    Critical = false;
    TDirectIoOpBase::ClearForRecycle();
}

void TDDiskActor::TPersistentBufferPartIoOp::ClearForRecycle() noexcept {
    PartCookie = 0;
    IsErase = false;
    IsRestore = false;
    TDirectIoOpBase::ClearForRecycle();
}

void TDDiskActor::TPersistentBufferPartIoOp::SelfRecycle() noexcept {
    Actor.ReturnOp(this);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor — pool AllocateOp / ReturnOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<T> TDDiskActor::AllocateOp() {
    auto& pool = [] (TDDiskActor& self) -> TSpscCircularQueue<std::unique_ptr<T>>& {
        if constexpr (std::is_same_v<T, TDDiskIoOp>) {
            return self.DdiskIoOpPool;
        } else {
            static_assert(std::is_same_v<T, TPersistentBufferPartIoOp>);
            return self.PersistentBufferPartIoOpPool;
        }
    }(*this);

    std::unique_ptr<T> op;
    if (!pool.TryPop(op)) {
        op = std::make_unique<T>(*this);
    }
    op->Reinit();
    return op;
}

template std::unique_ptr<TDDiskActor::TDDiskIoOp>
TDDiskActor::AllocateOp<TDDiskActor::TDDiskIoOp>();

template std::unique_ptr<TDDiskActor::TPersistentBufferPartIoOp>
TDDiskActor::AllocateOp<TDDiskActor::TPersistentBufferPartIoOp>();


void TDDiskActor::ReturnOp(TDDiskIoOp* op) {
    op->ClearForRecycle();
    if (!DdiskIoOpPool.TryPush(std::unique_ptr<TDDiskIoOp>(op))) {
        // unique_ptr destructor deletes anyway
    }
}

void TDDiskActor::ReturnOp(TPersistentBufferPartIoOp* op) {
    op->ClearForRecycle();
    if (!PersistentBufferPartIoOpPool.TryPush(std::unique_ptr<TPersistentBufferPartIoOp>(op))) {
        // unique_ptr destructor deletes anyway
    }
}


template <typename T>
void TDDiskActor::FillPool(TSpscCircularQueue<std::unique_ptr<T>>& pool) {
    for (ui32 i = 0; i < IoOpPoolCapacity; ++i) {
        pool.TryPush(std::make_unique<T>(*this));
    }
}

template void TDDiskActor::FillPool<TDDiskActor::TDDiskIoOp>(TSpscCircularQueue<std::unique_ptr<TDDiskActor::TDDiskIoOp>>&);
template void TDDiskActor::FillPool<TDDiskActor::TPersistentBufferPartIoOp>(TSpscCircularQueue<std::unique_ptr<TDDiskActor::TPersistentBufferPartIoOp>>&);

} // NKikimr::NDDisk
