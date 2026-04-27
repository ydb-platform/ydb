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

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const TWriteInstruction instr(record.GetInstruction());

        if (instr.PayloadId) {
            const TRope& data = ev->Get()->GetPayload(*instr.PayloadId);
            auto dataIter = data.Begin();
            if (dataIter.ContiguousSize() != data.size() ||
                    reinterpret_cast<uintptr_t>(dataIter.ContiguousData()) % DiskFormat->SectorSize != 0) {
                TStringStream ss;
                ss << "payload must be contiguous and aligned to " << DiskFormat->SectorSize << " bytes"
                    << ", contiguousSize# " << dataIter.ContiguousSize()
                    << " dataSize# " << data.size()
                    << " aligned# " << (reinterpret_cast<uintptr_t>(dataIter.ContiguousData()) % DiskFormat->SectorSize == 0);
                SendReply(*ev, std::make_unique<TEvWriteResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    ss.Str()));
                Counters.Interface.Write.Request(0);
                Counters.Interface.Write.Reply(false);
                return;
            }
        }

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty() || !chunkRef.ChunkIdx) {
            if (chunkRef.PendingEventsForChunk.empty() && !chunkRef.ChunkIdx) {
                IssueChunkAllocation(creds.TabletId, selector.VChunkIndex);
            }
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        Counters.Interface.Write.Request(selector.Size);

        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Write",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span
            .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size);

        TRope data;
        if (instr.PayloadId) {
            data = ev->Get()->GetPayload(*instr.PayloadId);
        }

        Y_ABORT_UNLESS(data.size() == selector.Size);

        auto offset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TDDiskIoOp>(ev.Get());
        op->SetSpan(std::move(span));
        op->PrepareWrite(std::move(data), offset, chunkRef.ChunkIdx, selector.OffsetInBytes);

        DirectUringOp(op);
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkWriteRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD07,
            "TDDiskActor::Handle(TEvChunkWriteRawResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        // TODO: should we really abort or propagate the error?
        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        auto it = WriteCallbacks.find(ev->Cookie);
        Y_ABORT_UNLESS(it != WriteCallbacks.end());

        std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
        WriteCallbacks.erase(it);

        // fill the op with result and finish via common completion path
        Y_DEBUG_ABORT_UNLESS(op->GetTotalSize() <= static_cast<ui64>(Max<i32>()));
        op->SetResult(static_cast<i32>(op->GetTotalSize()));

        op.release()->OnComplete(TActorContext::ActorSystem());
    }

    void TDDiskActor::Handle(TEvRead::TPtr ev) {
        STLOG(PRI_TRACE, BS_DDISK, BSDD21,
            "TDDiskActor::Handle(TEvRead)", (DDiskId, DDiskId), (Msg, ev->Get()->Record));

        if (!CheckQuery(*ev, &Counters.Interface.Read)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        TRope result;

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty()) {
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        Counters.Interface.Read.Request(selector.Size);

        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Read",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span
            .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size);

        if (!chunkRef.ChunkIdx) {
            auto zero = TRcBuf::Uninitialized(selector.Size);
            memset(zero.GetDataMut(), 0, zero.size());
            result.Insert(result.End(), std::move(zero));
            Counters.Interface.Read.Reply(true, selector.Size, 0);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                std::move(result)));
            return;
        }

        auto offset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TDDiskIoOp>(ev.Get());
        op->SetSpan(std::move(span));
        op->PrepareRead(selector.Size, offset, chunkRef.ChunkIdx, selector.OffsetInBytes);

        DirectUringOp(op);
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkReadRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD08,
            "TDDiskActor::Handle(TEvChunkReadRawResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        // TODO: should we really abort or propagate the error?
        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        auto it = ReadCallbacks.find(ev->Cookie);
        Y_ABORT_UNLESS(it != ReadCallbacks.end());

        std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
        ReadCallbacks.erase(it);

        // fill the op with result and finish via common completion path
        Y_DEBUG_ABORT_UNLESS(op->GetTotalSize() <= static_cast<ui64>(Max<i32>()));
        op->SetResult(static_cast<i32>(op->GetTotalSize()), std::move(msg.Data));

        op.release()->OnComplete(TActorContext::ActorSystem());
    }

    bool TDDiskActor::DirectUringOpImpl(std::unique_ptr<TDirectIoOpBase>& op, bool flush) {
#if defined(__linux__)
        Y_ABORT_UNLESS(UringRouter);

        // this is our main/regular path
        bool submitted = false;
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
        }

        // note, we want to flush anyway (e.g. previous operations)
        if (flush) {
            // with SQ polling – usually no syscall
            UringRouter->Flush();
        }

        return submitted;
#else
        Y_UNUSED(op);
        Y_UNUSED(flush);
#endif
        return false;
    }

    void TDDiskActor::DirectUringOp(std::unique_ptr<TDirectIoOpBase>& op, bool flush, bool isShort) {
        if (Y_LIKELY(!isShort)) {
            switch (op->GetOperationType()) {
            case NPDisk::TUringOperationBase::EREAD:
                Counters.DirectIO.Read.Request(op->GetTotalSize());
                break;
            case NPDisk::TUringOperationBase::EWRITE:
                Counters.DirectIO.Write.Request(op->GetTotalSize());
                break;
            default:
                Y_ABORT("Unknown OperationType");
            }
        }

#if defined(__linux__)
        if (Y_LIKELY(UringRouter)) {
            bool submitted = DirectUringOpImpl(op, flush);
            if (submitted) {
                Counters.DirectIO.RunningCount->Inc();
            } else {
                DirectIoQueue.push(std::move(op));
                Counters.DirectIO.QueueSize->Inc();
                ScheduleIoSubmitWakeup();
            }

            return;
        }
#endif

        Counters.DirectIO.RunningCount->Inc();

        // fallback path: either not linux or uring disabled / not available
        Y_UNUSED(flush);

        switch (op->GetOperationType()) {
        case NPDisk::TUringOperationBase::EREAD:
            SendPDiskRead(std::move(op));
            return;
        case NPDisk::TUringOperationBase::EWRITE:
            SendPDiskWrite(std::move(op));
            return;
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

        DirectUringOp(op, true, true);
    }

    void TDDiskActor::ScheduleIoSubmitWakeup() {
#if defined(__linux__)
        if (DirectIoQueue.empty()) {
            return;
        }

        ui32 currentInflight = UringRouter->GetInflight();

        // TODO: move to constants or config?
        const ui32 opMinLatencyUs = 10;
        const ui32 opsInParallel = 4;
        const ui32 inDiskInflight = 128;

        if (currentInflight <= inDiskInflight) {
            // for some reason we failed to submit, but there should have been space
            // in the router. It's OK to retry just a little bit later
            Schedule(TDuration::MicroSeconds(opMinLatencyUs), new TEvents::TEvWakeup(EWakeupTag::WakeupIoSubmitQueue));
            return;
        }

        const ui32 opsToWait = currentInflight - inDiskInflight;
        const ui32 usecWait = (opsToWait + opsInParallel - 1) * opMinLatencyUs / opsInParallel;
        Schedule(TDuration::MicroSeconds(usecWait), new TEvents::TEvWakeup(EWakeupTag::WakeupIoSubmitQueue));
#else
        Y_UNUSED(this);
#endif
    }

    void TDDiskActor::ProcessIoSubmitQueue() {
#if defined(__linux__)
        Y_ABORT_UNLESS(UringRouter);

        while (!DirectIoQueue.empty()) {
            auto& op = DirectIoQueue.front();
            auto queueTime = op->TimePassed();
            if (!DirectUringOpImpl(op, false)) {
                break;
            }
            DirectIoQueue.pop();
            Counters.DirectIO.QueueTime->Collect(queueTime);
            Counters.DirectIO.QueueSize->Dec();
            Counters.DirectIO.RunningCount->Inc();
        }

        // flush unconditionally
        UringRouter->Flush();

        ScheduleIoSubmitWakeup();
#else
        Y_UNUSED(this);
#endif
    }

    void TDDiskActor::HandleWakeup(TEvents::TEvWakeup::TPtr &ev) {
        switch (ev->Get()->Tag) {
            case EWakeupTag::WakeupIoSubmitQueue: {
                ProcessIoSubmitQueue();
                break;
            }
            case EWakeupTag::WakeupUpdateFreeSpaceInfo: {
                UpdateFreeSpaceInfo();
                break;
            }
        }
    }

} // NKikimr::NDDisk
