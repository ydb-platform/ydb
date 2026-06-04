#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/overloaded.h>
#include <util/stream/format.h>

#include <cerrno>

namespace NKikimr::NDDisk {

static constexpr size_t MaxRwCount = 0x7ffff000ULL; // INT_MAX & PAGE_MASK on 4K pages, ~ 2 GiB
static constexpr size_t MinBlockSize = 4096;

using TReplyStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

namespace {

// a poor error mapping (we can't map io_uring errors 1:1 to our errors)
TReplyStatus::E UringErrorToStatus(i32 result, NPDisk::TUringOperationBase::EOperationType opType) {
    const int err = -result;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDirectIoOpBase
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TDDiskActor::TDirectIoOpBase::TDirectIoOpBase(TDDiskActor& actor)
    : Actor(actor)
    , DDiskId(actor.SelfId())
    , StartTs(HPNow())
{}

TDDiskActor::TDirectIoOpBase::~TDirectIoOpBase() = default;

void TDDiskActor::TDirectIoOpBase::OnComplete(NActors::TActorSystem* actorSystem) noexcept
{
    std::unique_ptr<TDirectIoOpBase> guard(this);

    Actor.Counters.DirectIO.RunningCount->Dec();

    const size_t operationBytes = GetOperationBytes();
    const auto opType = GetOperationType();
    i32 result = GetResult();
    const double requestTimeMs = TimePassed();

    // note, we assume there is no short read/write with zero bytes,
    // otherwise we might loop forever on the short path
    if (Y_UNLIKELY(result == 0 && operationBytes > 0)) {
        result = -EIO;
        SetResult(result);
    }

    size_t bytesProcessed = 0;
    if (result >= 0) {
        bytesProcessed = static_cast<ui32>(result);
    }

    if (result < 0 || bytesProcessed == operationBytes) {
        switch (opType) {
        case TUringOperationBase::EREAD:
            Actor.Counters.DirectIO.Read.Done(GetTotalSize(), requestTimeMs);
            break;
        case TUringOperationBase::EWRITE:
            Actor.Counters.DirectIO.Write.Done(GetTotalSize(), requestTimeMs);
            break;
        default:
            Y_ABORT("Unknown OperationType");
        }
    }

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
        LOG_ERROR_S(*actorSystem, NKikimrServices::BS_DDISK, reason);
        Reply(actorSystem, UringErrorToStatus(result, opType), std::move(reason));
        Y_UNUSED(guard.release());
        SelfRecycle();
        return;
    }

    if (bytesProcessed == operationBytes) {
        Reply(actorSystem, TReplyStatus::OK);
        Y_UNUSED(guard.release());
        SelfRecycle();
        return;
    }

    // Below is an unlikely scenario: either short operation because of interrupt (or other reason),
    // or we have requested more bytes than i32 cqe->res (here it is Result) can report.

    this->AdvanceIov(bytesProcessed);

    switch (opType) {
    case TUringOperationBase::EREAD:
        Actor.Counters.DirectIO.ShortReads->Inc();
        break;
    case TUringOperationBase::EWRITE:
        Actor.Counters.DirectIO.ShortWrites->Inc();
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }

    // We can't initiate operation here, because ring is 1-to-1 between DDisk actor and the kernel,
    // while this is executed by the completion thread.
    auto ddiskId = DDiskId;
    auto ev = std::make_unique<TDDiskActor::TEvPrivate::TEvShortIO>(std::move(guard));
    actorSystem->Send(new IEventHandle(ddiskId, {}, ev.release()));
}

void TDDiskActor::TDirectIoOpBase::OnDrop() noexcept {
    std::unique_ptr<TDirectIoOpBase> guard(this);

    Actor.Counters.DirectIO.RunningCount->Dec();

    switch (GetOperationType()) {
    case TUringOperationBase::EREAD:
        Actor.Counters.DirectIO.Read.Done(GetTotalSize());
        break;
    case TUringOperationBase::EWRITE:
        Actor.Counters.DirectIO.Write.Done(GetTotalSize());
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }

    Y_UNUSED(guard.release());
    SelfRecycle();
}

void TDDiskActor::TDirectIoOpBase::PrepareWrite(TRope&& data, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset) {
    Y_ABORT_UNLESS(data.size() <= MaxRwCount);
    const size_t dataSize = data.size();
    Data.reset();

    // Zero-copy path: if the payload is contiguous and page-aligned, reuse the buffer directly.
    auto iter = data.Begin();
    if (iter.ContiguousSize() == data.size() &&
        (reinterpret_cast<uintptr_t>(iter.ContiguousData()) & (MinBlockSize - 1)) == 0) {
        AlignedDataHolder = iter.GetChunk(); // zero-copy: ref-count bump
    } else {
        AlignedDataHolder = TRcBuf::UninitializedPageAligned(dataSize);
        data.Begin().ExtractPlainDataAndAdvance(AlignedDataHolder.GetDataMut(), dataSize);
    }

    SetOperationType(EWRITE);

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

TRope TDDiskActor::TDirectIoOpBase::ExtractData() {
    if (Data) {
        return std::move(*Data);
    }

    return TRope(std::move(AlignedDataHolder));
}

double TDDiskActor::TDirectIoOpBase::TimePassed() const {
    return HPMilliSecondsFloat(HPNow() - StartTs);
}

void TDDiskActor::TDirectIoOpBase::SetResult(i32 result, TRope&& data) {
    SetResult(result);
    Data = std::move(data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TDDiskIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TDDiskIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status,
        TString reason) noexcept {
    const double requestTimeMs = TimePassed();

    std::unique_ptr<IEventBase> reply;
    TRope data;
    bool isOk = status == TReplyStatus::OK;
    std::optional<TString> errorReason;
    if (reason) {
        errorReason = std::move(reason);
    }

    switch (GetOperationType()) {
    case TUringOperationBase::EREAD: {
        if (status == TReplyStatus::OK) {
            data = ExtractData();
        }
        reply = std::make_unique<TEvReadResult>(status, errorReason, std::move(data));
        Actor.Counters.Interface.Read.Reply(isOk, GetTotalSize(), requestTimeMs);
        break;
    }
    case TUringOperationBase::EWRITE: {
        reply = std::make_unique<TEvWriteResult>(status, errorReason);
        Actor.Counters.Interface.Write.Reply(isOk, GetTotalSize(), requestTimeMs);
        break;
    }
    default:
        Y_ABORT("Unknown OperationType");
    }

    NWilson::TSpan& span = GetSpan();

    auto h = std::make_unique<IEventHandle>(GetOriginalRequester(), DDiskId, reply.release(),
        0, GetCookie(), nullptr, span.GetTraceId());
    const auto& session = GetInterconnectSession();
    if (session) {
        h->Rewrite(TEvInterconnect::EvForward, session);
    }
    span.End();
    actorSystem->Send(h.release());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TPersistentBufferPartIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TPersistentBufferPartIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status,
        TString reason) noexcept {
    std::unique_ptr<IEventBase> reply;
    const auto opType = GetOperationType();
    const i32 result = GetResult();
    if (status == TReplyStatus::OVERLOADED) {
        if (!reason) {
            reason = "io_uring SQ ring full (short I/O retry)";
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
            TRope data = ExtractData();
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

void TDDiskActor::TDirectIoOpBase::Reinit(const IEventHandle* ev) {
    ResetSubmissionState();
    StartTs = HPNow();
    if (ev) {
        OriginalRequester = ev->Sender;
        InterconnectSession = ev->InterconnectSession;
        Cookie = ev->Cookie;
    } else {
        OriginalRequester = {};
        InterconnectSession = {};
        Cookie = 0;
    }
    ChunkIdx = 0;
    ChunkOffsetInBytes = 0;
}

void TDDiskActor::TDirectIoOpBase::ClearForRecycle() noexcept {
    AlignedDataHolder = {};
    Data.reset();
    Span = {};
}

void TDDiskActor::TDDiskIoOp::SelfRecycle() noexcept {
    Actor.ReturnOp(this);
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

void TDDiskActor::TInternalSyncWriteOp::ClearForRecycle() noexcept {
    SyncId = 0;
    RequestId = 0;
    SegmentBegin = 0;
    SegmentEnd = 0;
    TDirectIoOpBase::ClearForRecycle();
}

void TDDiskActor::TInternalSyncWriteOp::SelfRecycle() noexcept {
    Actor.ReturnOp(this);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TInternalSyncWriteOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TInternalSyncWriteOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status,
        TString reason) noexcept {
    const i32 result = GetResult();

    if (status == TReplyStatus::OVERLOADED) {
        if (!reason) {
            reason = "io_uring SQ ring full (short I/O retry)";
        }
    } else if (status != TReplyStatus::OK) {
        if (!reason) {
            if (result < 0) {
                reason = TStringBuilder()
                    << "write failed: " << strerror(-result)
                    << " (errno " << (-result) << ")";
            } else {
                reason = "write failed";
            }
        }
    }

    actorSystem->Send(
        DDiskId,
        new TEvPrivate::TEvInternalSyncWriteResult(
            SyncId,
            RequestId,
            SegmentBegin,
            SegmentEnd,
            status,
            std::move(reason)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor — pool AllocateOp / ReturnOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<T> TDDiskActor::AllocateOp(const IEventHandle* ev) {
    auto& pool = [] (TDDiskActor& self) -> TSpscCircularQueue<std::unique_ptr<T>>& {
        if constexpr (std::is_same_v<T, TDDiskIoOp>) {
            return self.DdiskIoOpPool;
        } else if constexpr (std::is_same_v<T, TPersistentBufferPartIoOp>) {
            return self.PersistentBufferPartIoOpPool;
        } else {
            static_assert(std::is_same_v<T, TInternalSyncWriteOp>);
            return self.InternalSyncWriteOpPool;
        }
    }(*this);

    std::unique_ptr<T> op;
    if (!pool.TryPop(op)) {
        op = std::make_unique<T>(*this);
    }
    op->Reinit(ev);
    return op;
}

template std::unique_ptr<TDDiskActor::TDDiskIoOp>
TDDiskActor::AllocateOp<TDDiskActor::TDDiskIoOp>(const IEventHandle*);

template std::unique_ptr<TDDiskActor::TPersistentBufferPartIoOp>
TDDiskActor::AllocateOp<TDDiskActor::TPersistentBufferPartIoOp>(const IEventHandle*);

template std::unique_ptr<TDDiskActor::TInternalSyncWriteOp>
TDDiskActor::AllocateOp<TDDiskActor::TInternalSyncWriteOp>(const IEventHandle*);

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

void TDDiskActor::ReturnOp(TInternalSyncWriteOp* op) {
    op->ClearForRecycle();
    if (!InternalSyncWriteOpPool.TryPush(std::unique_ptr<TInternalSyncWriteOp>(op))) {
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
template void TDDiskActor::FillPool<TDDiskActor::TInternalSyncWriteOp>(TSpscCircularQueue<std::unique_ptr<TDDiskActor::TInternalSyncWriteOp>>&);

} // NKikimr::NDDisk
