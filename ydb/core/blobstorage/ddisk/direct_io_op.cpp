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

#include <cerrno>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_DDISK

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

    // EAGAIN/ENOMEM/ENOSPC on integrity/formatting I/O must not brick the DDisk: retry the same op
    // (buffers still owned here) through the short-I/O path. Defer Done() until the retry
    // completes or a hard error is reported.
    if (Y_UNLIKELY(result < 0 && IsCriticalDDiskIo()
            && UringErrorToStatus(result, opType) == TReplyStatus::OVERLOADED)) {
        auto ev = std::make_unique<TDDiskActor::TEvPrivate::TEvShortIO>(std::move(guard));
        actorSystem->Send(new IEventHandle(DDiskId, {}, ev.release()));
        return;
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
        YDB_LOG_ERROR_CTX(*actorSystem, reason);
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

    // Defer the retry through the actor so ownership and DDisk accounting stay
    // on the normal submission path.
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

TRope TDDiskActor::TDirectIoOpBase::ExtractData() {
    if (Data) {
        return std::move(*Data);
    }

    return TRope(std::move(AlignedDataHolder));
}

void TDDiskActor::TDirectIoOpBase::ApplyReadUsedBlocksMask(TRope& data) noexcept {
    if (!ReadUsedBlocksMask) {
        return;
    }

    // The buffer was just read from disk and is exclusively ours, so mutating without COW is safe.
    // On the uring path it is a single contiguous TRcBuf (AlignedDataHolder); on the PDisk fallback
    // path a non-contiguous rope would be compacted here (rare and small).
    auto span = data.UnsafeGetContiguousSpanMut();
    const size_t numBlocks = span.size() / IntegrityUnitSize;
    for (size_t i = 0; i < numBlocks; ++i) {
        if (!ReadUsedBlocksMask->Get(i)) {
            memset(span.data() + i * IntegrityUnitSize, 0, IntegrityUnitSize);
        }
    }
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
    TRope data;

    switch (GetOperationType()) {
    case TUringOperationBase::EREAD: {
        if (status == TReplyStatus::OK) {
            data = ExtractData();
            ApplyReadUsedBlocksMask(data);
        }
        break;
    }
    case TUringOperationBase::EWRITE:
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }

    actorSystem->Send(DDiskId, new TEvPrivate::TEvDDiskIoResult(
        GetOperationType(), status, std::move(reason), std::move(data),
        GetOriginalRequester(), GetInterconnectSession(), GetCookie(), ExtractSpan(),
        GetTotalSize(), requestTimeMs, TabletId, VChunkIndex, HasChunkKey,
        IntegrityOperationId, std::move(Checksums)));
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
            reason = "io_uring request temporarily overloaded (short I/O retry)";
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
    ReadUsedBlocksMask.reset();
}

void TDDiskActor::TDirectIoOpBase::ClearForRecycle() noexcept {
    AlignedDataHolder = {};
    Data.reset();
    Span = {};
    ReadUsedBlocksMask.reset();
}

void TDDiskActor::TDDiskIoOp::SelfRecycle() noexcept {
    Actor.ReturnOp(this);
}

void TDDiskActor::TDDiskIoOp::ClearForRecycle() noexcept {
    TabletId = 0;
    VChunkIndex = 0;
    HasChunkKey = false;
    IntegrityOperationId = 0;
    Checksums.clear();
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

void TDDiskActor::TInternalSyncWriteOp::ClearForRecycle() noexcept {
    SyncId = 0;
    RequestId = 0;
    SegmentBegin = 0;
    SegmentEnd = 0;
    IntegrityOperationId = 0;
    TDirectIoOpBase::ClearForRecycle();
}

void TDDiskActor::TInternalSyncWriteOp::SelfRecycle() noexcept {
    Actor.ReturnOp(this);
}

void TDDiskActor::TIntegrityIoOp::ClearForRecycle() noexcept {
    IoId = 0;
    TDirectIoOpBase::ClearForRecycle();
}

void TDDiskActor::TIntegrityIoOp::SelfRecycle() noexcept {
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
            reason = "io_uring request temporarily overloaded (short I/O retry)";
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
            IntegrityOperationId,
            status,
            std::move(reason)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TIntegrityIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TIntegrityIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status,
        TString reason) noexcept {
    const i32 result = GetResult();
    TRope data;

    if (status != TReplyStatus::OK && !reason) {
        if (result < 0) {
            reason = TStringBuilder()
                << "integrity I/O failed: " << strerror(-result)
                << " (errno " << (-result) << ")";
        } else {
            reason = "integrity I/O failed";
        }
    } else if (status == TReplyStatus::OK && GetOperationType() == TUringOperationBase::EREAD) {
        data = ExtractData();
    }

    actorSystem->Send(DDiskId, new TEvPrivate::TEvIntegrityIoResult(
        IoId, status, std::move(reason), std::move(data),
        GetOperationType() == TUringOperationBase::EREAD));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TChunkFormatIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TChunkFormatIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status,
        TString reason) noexcept {
    if (status != TReplyStatus::OK && !reason) {
        const i32 result = GetResult();
        if (result < 0) {
            reason = TStringBuilder() << "chunk zero-format write failed: " << strerror(-result)
                << " (errno " << (-result) << ")";
        } else {
            reason = "chunk zero-format write failed";
        }
    }
    actorSystem->Send(DDiskId, new TEvPrivate::TEvChunkFormatIoResult(
        ChunkIdx, OffsetInBytes, Size, status, std::move(reason)));
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
        } else if constexpr (std::is_same_v<T, TIntegrityIoOp>) {
            return self.IntegrityIoOpPool;
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

template std::unique_ptr<TDDiskActor::TIntegrityIoOp>
TDDiskActor::AllocateOp<TDDiskActor::TIntegrityIoOp>(const IEventHandle*);

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

void TDDiskActor::ReturnOp(TIntegrityIoOp* op) {
    op->ClearForRecycle();
    if (!IntegrityIoOpPool.TryPush(std::unique_ptr<TIntegrityIoOp>(op))) {
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
template void TDDiskActor::FillPool<TDDiskActor::TIntegrityIoOp>(TSpscCircularQueue<std::unique_ptr<TDDiskActor::TIntegrityIoOp>>&);

} // NKikimr::NDDisk
