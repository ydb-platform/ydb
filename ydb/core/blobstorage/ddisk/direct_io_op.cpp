#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <util/generic/overloaded.h>
#include <ydb/core/util/stlog.h>

#include <cerrno>

namespace NKikimr::NDDisk {
#if defined(__linux__)

void TDDiskActor::TDirectIoOpBase::SetData(TRope&& data) {
    // Zero-copy path: if the payload is contiguous and page-aligned, reuse the buffer directly.
    // TODO: should we check page size? And for large writes and huge pages should properly align?
    auto iter = data.Begin();
    if (iter.ContiguousSize() == data.size()) {
        AlignedDataHolder = iter.GetChunk(); // zero-copy: ref-count bump
    } else {
        AlignedDataHolder = TRcBuf::UninitializedPageAligned(data.size());
        data.Begin().ExtractPlainDataAndAdvance(AlignedDataHolder.GetDataMut(), data.size());
    }
}


TDDiskActor::TDirectIoOpBase::TDirectIoOpBase(std::atomic<ui32>& inFlightCount, TCounters& counters)
    : InFlightCount(inFlightCount)
    , Counters(counters)
{
    OnComplete = &TDirectIoOpBase::OnDirectIoComplete;
    OnDrop = &TDirectIoOpBase::OnDirectIoDrop;
}

void TDDiskActor::TDDiskIoOp::Reply(NActors::TActorSystem* actorSystem, bool shortIoError) {
    std::unique_ptr<IEventBase> reply;
    if (Result >= 0 && !shortIoError) {
        if (IsRead) {
            TRope data(std::move(AlignedDataHolder));
            reply = std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, std::move(data));
        } else {
            reply = std::make_unique<TEvWriteResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        }
    } else {
        auto status = shortIoError ? NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED : UringErrorToStatus(Result, IsRead);
        TString reason;
        if (shortIoError) {
            reason = "io_uring SQ ring full (short I/O retry)";
        } else {
            reason = TStringBuilder()
            << (IsRead ? "read" : "write")
            << " failed: " << strerror(-Result)
            << " (errno " << (-Result) << ")";
        }
        if (IsRead) {
            reply = std::make_unique<TEvReadResult>(status, reason);
        } else {
            reply = std::make_unique<TEvWriteResult>(status, reason);
        }
    }

    auto h = std::make_unique<IEventHandle>(Sender, DDiskId, reply.release(),
        0, Cookie, nullptr, Span.GetTraceId());
    if (InterconnectSession) {
        h->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
    }
    Span.End();
    actorSystem->Send(h.release());

    const bool ok = Result >= 0 && !shortIoError;
    if (IsRead) {
        Counters.Interface.Read.Reply(ok, ok ? Size : 0);
    } else {
        Counters.Interface.Write.Reply(ok, ok ? Size : 0);
    }
}

void TDDiskActor::TPersistentBufferPartIoOp::Reply(NActors::TActorSystem* actorSystem, bool shortIoError) {
    std::unique_ptr<IEventBase> reply;
    NKikimrBlobStorage::NDDisk::TReplyStatus::E status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
    TString reason;
    if (shortIoError) {
        status = NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED;
        reason = "io_uring SQ ring full (short I/O retry)";
    }
    if (Result < 0) {
        status = UringErrorToStatus(Result, IsRead);
        reason = TStringBuilder()
            << (IsRead ? "read" : "write")
            << " failed: " << strerror(-Result)
            << " (errno " << (-Result) << ")";
    }

    if (IsRead) {
        reply = std::make_unique<TEvPrivate::TEvReadPersistentBufferPart>(Cookie, PartCookie, status, reason, std::move(AlignedDataHolder));
    } else {
        reply = std::make_unique<TEvPrivate::TEvWritePersistentBufferPart>(Cookie, PartCookie, status, reason, IsErase);
    }
    Y_ABORT_UNLESS(!InterconnectSession);
    actorSystem->Send(DDiskId, reply.release());
}

#endif // defined(__linux__)

} // NKikimr::NDDisk
