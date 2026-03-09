#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <util/generic/overloaded.h>
#include <ydb/core/util/stlog.h>

#include <cerrno>

namespace NKikimr::NDDisk {

static constexpr size_t MaxRwCount = 0x7ffff000ULL; // INT_MAX & PAGE_MASK on 4K pages, ~ 2 GiB

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

TDDiskActor::TDirectIoOpBase::TDirectIoOpBase(const TActorId& ddiskId,
                                              std::atomic<ui32>& inFlightCount,
                                              TCounters& counters,
                                              const IEventHandle* ev)
    : DDiskId(ddiskId)
    , InFlightCount(inFlightCount)
    , Counters(counters)
{
    if (ev) {
        OriginalRequester = ev->Sender;
        InterconnectSession = ev->InterconnectSession;
        Cookie = ev->Cookie;
    }

    InFlightCount.fetch_add(1, std::memory_order_relaxed);
}

TDDiskActor::TDirectIoOpBase::~TDirectIoOpBase() {
    InFlightCount.fetch_sub(1, std::memory_order_relaxed);
}

void TDDiskActor::TDirectIoOpBase::OnComplete(NActors::TActorSystem* actorSystem) noexcept
{
    // selfkill at the very end. TODO: return to the pool
    std::unique_ptr<TDirectIoOpBase> guard(this);

    const size_t operationBytes = GetOperationBytes();
    const auto opType = GetOperationType();
    i32 result = GetResult();

    // note, we assume there is no short read/write with zero bytes,
    // otherwise we might loop forever on the short path
    if (Y_UNLIKELY(result == 0 && operationBytes > 0)) {
        result = -EIO;
        SetResult(result);
    }

    if (Y_UNLIKELY(result < 0)) {
        Reply(actorSystem, UringErrorToStatus(result, opType));
        return;
    }

    auto bytesProcessed = static_cast<ui32>(result);

    if (bytesProcessed == operationBytes) {
        Reply(actorSystem, TReplyStatus::OK);
        return;
    }

    // Below is an unlikely scenario: either short operation because of interrupt (or other reason),
    // or we have requested more bytes than i32 cqe->res (here it is Result) can report.

    this->AdvanceIov(bytesProcessed);

    switch (opType) {
    case TUringOperationBase::EREAD:
        Counters.DirectIO.ShortReads->Inc();
        break;
    case TUringOperationBase::EWRITE:
        Counters.DirectIO.ShortWrites->Inc();
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }

    // We can't initiate operation here, because ring is 1-to-1 between DDisk actor and the kernel,
    // while this is executed by the completion thread.

    // !!! we move this object and should not use it further
    // thus have to copy
    auto ddiskId = DDiskId;
    auto ev = std::make_unique<TDDiskActor::TEvPrivate::TEvShortIO>(std::move(guard));
    actorSystem->Send(new IEventHandle(ddiskId, {}, ev.release()));
}

void TDDiskActor::TDirectIoOpBase::OnDrop() noexcept {
    // TODO: return to the pool
    delete this;
}

void TDDiskActor::TDirectIoOpBase::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status) noexcept {
    std::unique_ptr<IEventBase> reply;
    const auto opType = GetOperationType();
    if (status == TReplyStatus::OK) {
        switch (opType) {
        case TUringOperationBase::EREAD: {
            TRope data = ExtractData();
            reply = std::make_unique<TEvReadResult>(
                TReplyStatus::OK, std::nullopt, std::move(data));
            Counters.Interface.Read.Reply(true, GetTotalSize());
            break;
        }
        case TUringOperationBase::EWRITE: {
            reply = std::make_unique<TEvWriteResult>(
                TReplyStatus::OK);
            Counters.Interface.Write.Reply(true, GetTotalSize());
            break;
        }
        default:
            Y_ABORT("Unknown OperationType");
        }
    } else {
        switch (opType) {
        case TUringOperationBase::EREAD:
            Counters.Interface.Read.Reply(false);
            reply = std::make_unique<TEvReadResult>(status);
            break;
        case TUringOperationBase::EWRITE:
            Counters.Interface.Write.Reply(false);
            reply = std::make_unique<TEvWriteResult>(status);
            break;
        default:
            Y_ABORT("Unknown OperationType");
        }
    }

    auto h = std::make_unique<IEventHandle>(OriginalRequester, DDiskId, reply.release(),
        0, Cookie, nullptr, Span.GetTraceId());
    if (InterconnectSession) {
        h->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
    }
    Span.End();
    actorSystem->Send(h.release());
}

void TDDiskActor::TDirectIoOpBase::PrepareWrite(TRope&& data, ui64 offset, TChunkIdx chunkIdx, ui32 chunkOffset) {
    Y_ABORT_UNLESS(data.size() <= MaxRwCount);
    Data.reset();

    // Zero-copy path: if the payload is contiguous and page-aligned, reuse the buffer directly.
    // TODO: should we check page size? And for large writes and huge pages should properly align?
    auto iter = data.Begin();
    if (iter.ContiguousSize() == data.size()) {
        AlignedDataHolder = iter.GetChunk(); // zero-copy: ref-count bump
    } else {
        AlignedDataHolder = TRcBuf::UninitializedPageAligned(data.size());
        data.Begin().ExtractPlainDataAndAdvance(AlignedDataHolder.GetDataMut(), data.size());
    }

    SetOperationType(EWRITE);
    PrepareIov(AlignedDataHolder.GetDataMut(), data.size(), offset);

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

void TDDiskActor::TDirectIoOpBase::SetResult(i32 result, TRope&& data) {
    SetResult(result);
    Data = std::move(data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDDiskActor::TPersistentBufferPartIoOp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TDDiskActor::TPersistentBufferPartIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status) noexcept {
    std::unique_ptr<IEventBase> reply;
    TString reason;
    const auto opType = GetOperationType();
    const i32 result = GetResult();
    if (status == TReplyStatus::OVERLOADED) {
        reason = "io_uring SQ ring full (short I/O retry)";
    } else if (status != TReplyStatus::OK) {
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

    switch (opType) {
        case TUringOperationBase::EREAD: {
            TRope data = ExtractData();
            reply = std::make_unique<TEvPrivate::TEvReadPersistentBufferPart>(
                GetCookie(), PartCookie, status, std::move(reason), std::move(data));
            break;
        }
        case TUringOperationBase::EWRITE:
            reply = std::make_unique<TEvPrivate::TEvWritePersistentBufferPart>(
                GetCookie(), PartCookie, status, reason, IsErase);
            break;
        default:
            Y_ABORT("Unknown OperationType");
    }

    actorSystem->Send(GetDDiskId(), reply.release());
}

void TDDiskActor::TInternalSyncWriteOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status) noexcept {
    TString reason;
    const i32 result = GetResult();

    if (status == TReplyStatus::OVERLOADED) {
        reason = "io_uring SQ ring full (short I/O retry)";
    } else if (status != TReplyStatus::OK) {
        if (result < 0) {
            reason = TStringBuilder()
                << "write failed: " << strerror(-result)
                << " (errno " << (-result) << ")";
        } else {
            reason = "write failed";
        }
    }

    actorSystem->Send(
        GetDDiskId(),
        new TEvPrivate::TEvInternalSyncWriteResult(
            SyncId,
            RequestId,
            SegmentBegin,
            SegmentEnd,
            status,
            std::move(reason)));
}

} // NKikimr::NDDisk
