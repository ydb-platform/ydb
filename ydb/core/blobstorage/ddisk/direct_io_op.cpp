#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/util/stlog.h>

#include <util/generic/overloaded.h>

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
                                              TCounters& counters,
                                              const IEventHandle* ev)
    : DDiskId(ddiskId)
    , Counters(counters)
    , StartTs(HPNow())
{
    if (ev) {
        OriginalRequester = ev->Sender;
        InterconnectSession = ev->InterconnectSession;
        Cookie = ev->Cookie;
    }
}

TDDiskActor::TDirectIoOpBase::~TDirectIoOpBase() = default;

void TDDiskActor::TDirectIoOpBase::OnComplete(NActors::TActorSystem* actorSystem) noexcept
{
    // selfkill at the very end. TODO: return to the pool
    std::unique_ptr<TDirectIoOpBase> guard(this);
    Counters.DirectIO.RunningCount->Dec();

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
        // we exclude short read/write, only final "iteration"
        // should update metrics
        switch (opType) {
        case TUringOperationBase::EREAD:
            Counters.DirectIO.Read.Done(GetTotalSize(), requestTimeMs);
            break;
        case TUringOperationBase::EWRITE:
            Counters.DirectIO.Write.Done(GetTotalSize(), requestTimeMs);
            break;
        default:
            Y_ABORT("Unknown OperationType");
        }
    }

    if (Y_UNLIKELY(result < 0)) {
        Reply(actorSystem, UringErrorToStatus(result, opType));
        return;
    }

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
    Counters.DirectIO.RunningCount->Dec();

    switch (GetOperationType()) {
    case TUringOperationBase::EREAD:
        Counters.DirectIO.Read.Done(GetTotalSize());
        break;
    case TUringOperationBase::EWRITE:
        Counters.DirectIO.Write.Done(GetTotalSize());
        break;
    default:
        Y_ABORT("Unknown OperationType");
    }

    delete this;
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

void TDDiskActor::TDDiskIoOp::Reply(NActors::TActorSystem* actorSystem, TReplyStatus::E status) noexcept {
    const double requestTimeMs = TimePassed();

    std::unique_ptr<IEventBase> reply;
    TRope data;
    bool isOk = status == TReplyStatus::OK;

    switch (GetOperationType()) {
    case TUringOperationBase::EREAD: {
        if (status == TReplyStatus::OK) {
            data = ExtractData();
        }
        reply = std::make_unique<TEvReadResult>(status, std::nullopt, std::move(data));
        Counters.Interface.Read.Reply(isOk, GetTotalSize(), requestTimeMs);
        break;
    }
    case TUringOperationBase::EWRITE: {
        reply = std::make_unique<TEvWriteResult>(status);
        Counters.Interface.Write.Reply(isOk, GetTotalSize(), requestTimeMs);
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
        DDiskId,
        new TEvPrivate::TEvInternalSyncWriteResult(
            SyncId,
            RequestId,
            SegmentBegin,
            SegmentEnd,
            status,
            std::move(reason)));
}

} // NKikimr::NDDisk
