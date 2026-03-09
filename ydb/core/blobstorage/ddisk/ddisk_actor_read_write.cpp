#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <util/generic/overloaded.h>
#include <ydb/core/util/stlog.h>

#include <cerrno>

namespace NKikimr::NDDisk {

    TDDiskActor::TPendingIoOp::TPendingIoOp(std::unique_ptr<TDirectIoOpBase> op)
        : Op(std::move(op))
    {}

    TDDiskActor::TPendingIoOp::TPendingIoOp(TPendingIoOp&&) noexcept = default;
    TDDiskActor::TPendingIoOp& TDDiskActor::TPendingIoOp::operator=(TPendingIoOp&&) noexcept = default;
    TDDiskActor::TPendingIoOp::~TPendingIoOp() = default;

    void TDDiskActor::SendPDiskWrite(std::unique_ptr<TDirectIoOpBase> op) {
        const ui64 cookie = NextCookie++;
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkWriteRaw(
            PDiskParams->Owner,
            PDiskParams->OwnerRound,
            op->GetChunkIdx(),
            op->GetChunkOffset(),
            op->ExtractData()), 0, cookie);

        WriteCallbacks.try_emplace(
            cookie,
            TPendingIoOp(std::move(op)));
    }

    void TDDiskActor::SendPDiskRead(std::unique_ptr<TDirectIoOpBase> op) {
        const ui64 cookie = NextCookie++;
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReadRaw(
            PDiskParams->Owner,
            PDiskParams->OwnerRound,
            op->GetChunkIdx(),
            op->GetChunkOffset(),
            op->GetTotalSize()), 0, cookie);

        ReadCallbacks.try_emplace(
            cookie,
            TPendingIoOp(std::move(op)));
    }

    void TDDiskActor::Handle(TEvWrite::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.Write)) {
            return;
        }

        // TODO: check that request is within a single chunk

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty() || !chunkRef.ChunkIdx) {
            if (chunkRef.PendingEventsForChunk.empty() && !chunkRef.ChunkIdx) {
                IssueChunkAllocation(creds.TabletId, selector.VChunkIndex);
            }
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        const TWriteInstruction instr(record.GetInstruction());

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Write",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size));

        TRope data;
        if (instr.PayloadId) {
            data = ev->Get()->GetPayload(*instr.PayloadId);
        }

        Y_ABORT_UNLESS(data.size() == selector.Size);

        Counters.Interface.Write.Request();

        auto offset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        // TODO: use pool
        std::unique_ptr<TDirectIoOpBase> op = std::make_unique<TDirectIoOpBase>(
            SelfId(), InFlightCount, Counters, ev.Get());
        op->SetSpan(std::move(span));
        op->PrepareWrite(std::move(data), offset, chunkRef.ChunkIdx, selector.OffsetInBytes);

        const bool submitted = DirectUringOp(op);
        if (!submitted) {
            op->GetSpan().End();
            Counters.Interface.Write.Reply(false);
            SendReply(*ev, std::make_unique<TEvWriteResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "io_uring SQ ring full"));
        }
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkWriteRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD07,
            "TDDiskActor::Handle(TEvChunkWriteRawResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        // TODO: should we really abort or propagate the error?
        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        const auto it = WriteCallbacks.find(ev->Cookie);
        Y_ABORT_UNLESS(it != WriteCallbacks.end());

        // fill the op with result
        auto* op = it->second.Op.get();
        Y_DEBUG_ABORT_UNLESS(op->GetTotalSize() <= static_cast<ui64>(Max<i32>()));
        op->SetResult(static_cast<i32>(op->GetTotalSize()));

        op->Reply(TActorContext::ActorSystem(), NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        WriteCallbacks.erase(it);
    }

    void TDDiskActor::Handle(TEvRead::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.Read)) {
            return;
        }

        // TODO: check that request is within a single chunk

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        TRope result;

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty()) {
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        Counters.Interface.Read.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Read",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size));

        if (!chunkRef.ChunkIdx) {
            auto zero = TRcBuf::Uninitialized(selector.Size);
            memset(zero.GetDataMut(), 0, zero.size());
            result.Insert(result.End(), std::move(zero));
            Counters.Interface.Read.Reply(true, selector.Size);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                std::move(result)));
            return;
        }

        auto offset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        // TODO: use pool
        std::unique_ptr<TDirectIoOpBase> op = std::make_unique<TDirectIoOpBase>(
            SelfId(), InFlightCount, Counters, ev.Get());
        op->SetSpan(std::move(span));
        op->PrepareRead(selector.Size, offset, chunkRef.ChunkIdx, selector.OffsetInBytes);

        const bool submitted = DirectUringOp(op);
        if (!submitted) {
            op->GetSpan().End();
            Counters.Interface.Read.Reply(false);
            SendReply(*ev, std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "io_uring SQ ring full"));
        }
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkReadRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD08,
            "TDDiskActor::Handle(TEvChunkReadRawResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        // TODO: should we really abort or propagate the error?
        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        const auto it = ReadCallbacks.find(ev->Cookie);

        // TODO: remove
        if (it == ReadCallbacks.end()) {
            const auto it2 = ReadCallbacksRaw.find(ev->Cookie);
            Y_ABORT_UNLESS(it2 != ReadCallbacksRaw.end());
            std::visit(TOverloaded{
                [&](TPendingRead& w) {
                    w.Callback(msg, std::move(w.Span));
                },
                [&](const TPersistentBufferPendingRead& callback) {
                    callback(msg);
                }
            }, it2->second);
            ReadCallbacksRaw.erase(it2);
            return;
        }

        Y_ABORT_UNLESS(it != ReadCallbacks.end());

        // fill the op with result
        auto* op = it->second.Op.get();
        Y_DEBUG_ABORT_UNLESS(op->GetTotalSize() <= static_cast<ui64>(Max<i32>()));
        op->SetResult(static_cast<i32>(op->GetTotalSize()), std::move(msg.Data));

        op->Reply(TActorContext::ActorSystem(), NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        ReadCallbacks.erase(it);
    }

    bool TDDiskActor::DirectUringOp(std::unique_ptr<TDirectIoOpBase>& op, bool flush) {
        // TODO: add queue?

        // TODO: we increase it in TDirectIoOpBase and decrease in destructor,
        // which is bad, because check later here
        if (InFlightCount.load(std::memory_order_relaxed) > MaxInFlight) {
            return false;
        }

#if defined(__linux__)
        if (Y_LIKELY(UringRouter)) {
            // this is our main/regular path
            bool submitted;
            switch (op->GetOperationType()) {
            case NPDisk::TUringOperationBase::EREAD:
                submitted = UringRouter->Read(op.get());
                break;
            case NPDisk::TUringOperationBase::EWRITE:
                submitted = UringRouter->Write(op.get());
                break;
            default:
                Y_ABORT("Unknown OperationType");
            }

            if (submitted) {
                Y_UNUSED(op.release());
                if (flush) {
                    // with SQ polling – usually no syscall
                    UringRouter->Flush();
                }
            }
            return submitted;
        }
#endif

        // fallback path: either not linux or uring disabled / not available
        Y_UNUSED(flush);

        switch (op->GetOperationType()) {
        case NPDisk::TUringOperationBase::EREAD:
            SendPDiskRead(std::move(op));
            return true;
        case NPDisk::TUringOperationBase::EWRITE:
            SendPDiskWrite(std::move(op));
            return true;
        default:
            Y_ABORT("Unknown OperationType");
        }
    }

    TDDiskActor::TEvPrivate::TEvShortIO::TEvShortIO(std::unique_ptr<TDirectIoOpBase> op)
        : Op(std::move(op))
    {}

    TDDiskActor::TEvPrivate::TEvShortIO::~TEvShortIO() = default;

    void TDDiskActor::HandleShortIO(TEvPrivate::TEvShortIO::TPtr ev) {
        std::unique_ptr<TDirectIoOpBase> op = std::move(ev->Get()->Op);

        bool submitted = DirectUringOp(op);
        if (!submitted) {
            op->Reply(TActivationContext::ActorSystem(), NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED);
        }
    }

} // NKikimr::NDDisk
