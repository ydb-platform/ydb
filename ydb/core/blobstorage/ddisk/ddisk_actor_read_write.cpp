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
        YDB_LOG_TRACE_COMP(BS_DDISK, "TDDiskActor::Handle(TEvWrite)",
            {"marker", "BSDD50"},
            {"DDiskId", DDiskId},
            {"sender", ev->Sender},
            {"cookie", ev->Cookie});

        if (!CheckQuery(*ev, &Counters.Interface.Write)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const TWriteInstruction instr(record.GetInstruction());

        if (TabletChunkDeletionsInFlight.contains(creds.TabletId)) {
            Counters.Interface.Write.Request(selector.Size);
            Counters.Interface.Write.Reply(false, selector.Size);
            SendReply(*ev, std::make_unique<TEvWriteResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                "tablet chunk deletion is in flight"));
            return;
        }

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

                YDB_LOG_DEBUG_CTX_COMP(*TActivationContext::ActorSystem(), NKikimrServices::BS_DDISK, "Dump DDiskId: payload must be contiguous and aligned",
                    {"sectorSize", DiskFormat->SectorSize},
                    {"contiguousSize", dataIter.ContiguousSize()},
                    {"dataSize", data.size()},
                    {"aligned", (reinterpret_cast<uintptr_t>(dataIter.ContiguousData()) % DiskFormat->SectorSize == 0)},
                    {"DDiskId", DDiskId});

                SendReply(*ev, std::make_unique<TEvWriteResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    ss.Str()));
                Counters.Interface.Write.Request(selector.Size);
                Counters.Interface.Write.Reply(false, selector.Size);
                return;
            }
        }

        if (selector.OffsetInBytes % IntegrityUnitSize || selector.Size % IntegrityUnitSize) {
            Counters.Interface.Write.Request(selector.Size);
            Counters.Interface.Write.Reply(false, selector.Size);
            SendReply(*ev, std::make_unique<TEvWriteResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                "write offset and size must be aligned to 4 KiB"));
            return;
        }

        if (Config.EnableChecksums) {
            if (!HasRequiredBlockChecksums(record.ChecksumsSize(), selector.OffsetInBytes, selector.Size)) {
                if (record.ChecksumsSize() == 0) {
                    Counters.Checksums.WritesWithoutChecksums->Inc();
                }
                Counters.Interface.Write.Request(selector.Size);
                Counters.Interface.Write.Reply(false, selector.Size);
                SendReply(*ev, std::make_unique<TEvWriteResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    "one checksum per aligned 4 KiB block is required"));
                return;
            }

            // Validate before chunk allocation or data/integrity I/O. Parked events pass through this
            // check again when re-dispatched; the redundant validation is harmless.
            Y_ABORT_UNLESS(instr.PayloadId, "TEvWrite without a payload, but with checksums");

            const TRope& payload = ev->Get()->GetPayload(*instr.PayloadId);
            if (const auto result = ValidatePayloadChecksums(record, payload)) {
                const bool isCorrupted = result->Status == NKikimrBlobStorage::NDDisk::TReplyStatus::CORRUPTED;
                Counters.Interface.Write.Request(selector.Size);
                Counters.Interface.Write.Reply(false, selector.Size);
                if (isCorrupted) {
                    Counters.Checksums.ChecksumMismatch->Inc();
                }
                YDB_LOG_ERROR_COMP(NKikimrServices::BS_DDISK,
                    (isCorrupted
                        ? "TDDiskActor::Handle(TEvWrite) checksum mismatch"
                        : "TDDiskActor::Handle(TEvWrite) checksum count mismatch"),
                    {"marker", "BSDD52"},
                    {"DDiskId", DDiskId},
                    {"tabletId", creds.TabletId},
                    {"vChunkIndex", selector.VChunkIndex},
                    {"offsetInBytes", selector.OffsetInBytes},
                    {"checksumCount", result->ChecksumCount},
                    {"selectorSize", selector.Size},
                    {"blockIdx", result->MismatchedBlockIdx ? static_cast<i64>(*result->MismatchedBlockIdx) : -1});
                SendReply(*ev, std::make_unique<TEvWriteResult>(result->Status, result->ErrorReason));
                return;
            }
        }

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty() || !chunkRef.ChunkIdx) {
            // Park first: IssueChunkAllocation may place the extent synchronously from the
            // reserve and OpenDataChunkWritePath only drains already-queued events.
            const bool startAllocation = chunkRef.PendingEventsForChunk.empty() && !chunkRef.ChunkIdx;
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            if (startAllocation) {
                IssueChunkAllocation(creds.TabletId, selector.VChunkIndex);
            }
            return;
        }

        if (chunkRef.IntegrityExtentWriteInFlight) {
            chunkRef.PendingSerializedWrites.emplace(ev, "WaitIntegrityExtentWrite");
            return;
        }
        if (Config.EnableChecksums) {
            chunkRef.IntegrityExtentWriteInFlight = true;
        }

        Counters.Interface.Write.Request(selector.Size);
        const auto requestStartTs = HPNow();

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

        ui64 integrityOperationId = 0;
        if (Config.EnableChecksums) {
            std::vector<ui64> checksums(record.GetChecksums().begin(), record.GetChecksums().end());
            integrityOperationId = IntegrityManager->BeginBlocksWrite(
                {creds.TabletId, selector.VChunkIndex}, selector.OffsetInBytes, selector.Size, checksums);
            const bool inserted = PendingClientWrites.try_emplace(integrityOperationId).second;
            Y_ABORT_UNLESS(inserted);
            DrainIntegrityManager();
            const auto pendingIt = PendingClientWrites.find(integrityOperationId);
            if (pendingIt != PendingClientWrites.end() && pendingIt->second.IntegrityCompleted
                    && pendingIt->second.IntegrityStatus == TIntegrityManager::EOperationStatus::Corrupted) {
                pendingIt->second.DataResult.emplace(TParkedWriteReply{
                    .Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
                    .OriginalRequester = ev->Sender,
                    .InterconnectSession = ev->InterconnectSession,
                    .Cookie = ev->Cookie,
                    .Span = std::move(span),
                    .TotalSize = selector.Size,
                    .RequestTimeMs = HPMilliSecondsFloat(HPNow() - requestStartTs),
                    .TabletId = creds.TabletId,
                    .VChunkIndex = selector.VChunkIndex,
                });
                MaybeFinishClientWrite(integrityOperationId);
                return;
            }
        }

        auto offset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TDDiskIoOp>(ev.Get());
        auto* ddiskOp = static_cast<TDDiskIoOp*>(op.get());
        ddiskOp->SetChunkKey(creds.TabletId, selector.VChunkIndex);
        ddiskOp->SetIntegrityOperationId(integrityOperationId);
        op->SetSpan(std::move(span));
        op->PrepareWrite(std::move(data), offset, chunkRef.ChunkIdx, selector.OffsetInBytes);

        ++chunkRef.InFlightDataIo;
        DirectUringOp(op);
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkWriteRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG_COMP(BS_DDISK, "TDDiskActor::Handle(TEvChunkWriteRawResult)",
            {"marker", "BSDD07"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        auto it = WriteCallbacks.find(ev->Cookie);
        if (it == WriteCallbacks.end()) {
            Y_ABORT_UNLESS(IsBroken());
            return;
        }

        if (Y_UNLIKELY(IsBroken())) {
            std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
            WriteCallbacks.erase(it);
            op->SetResult(-EIO);
            op.release()->OnComplete(TActorContext::ActorSystem());
            return;
        }

        if (msg.Status != NKikimrProto::OK) {
            if (it->second.Op->IsCriticalDDiskIo()) {
                // A fallback integrity/format write is a DDisk failure, not a reason to enter
                // the passive PDisk-session termination state. Finish it through the same op path
                // as an io_uring EIO so the health latch is published before any success reply.
                std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
                WriteCallbacks.erase(it);
                op->SetResult(-EIO);
                op.release()->OnComplete(TActorContext::ActorSystem());
                return;
            }
            if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvChunkWriteRawResult)")) {
                return;
            }
        }

        std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
        WriteCallbacks.erase(it);

        // fill the op with result and finish via common completion path
        Y_DEBUG_ABORT_UNLESS(op->GetTotalSize() <= static_cast<ui64>(Max<i32>()));
        op->SetResult(static_cast<i32>(op->GetTotalSize()));

        op.release()->OnComplete(TActorContext::ActorSystem());
    }

    void TDDiskActor::Handle(TEvRead::TPtr ev) {
        YDB_LOG_TRACE_COMP(BS_DDISK, "TDDiskActor::Handle(TEvRead)",
            {"marker", "BSDD21"},
            {"DDiskId", DDiskId},
            {"msg", ev->Get()->Record});

        if (!CheckQuery(*ev, &Counters.Interface.Read)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        if (selector.OffsetInBytes % IntegrityUnitSize != 0
                || selector.Size % IntegrityUnitSize != 0) {
            Counters.Interface.Read.Request(selector.Size);
            Counters.Interface.Read.Reply(false, selector.Size);
            SendReply(*ev, std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                "read offset and size must be aligned to the 4 KiB integrity unit"));
            return;
        }

        if (TabletChunkDeletionsInFlight.contains(creds.TabletId)) {
            Counters.Interface.Read.Request(selector.Size);
            Counters.Interface.Read.Reply(false, selector.Size);
            SendReply(*ev, std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY,
                "tablet chunk deletion is in flight"));
            return;
        }

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty()) {
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        Counters.Interface.Read.Request(selector.Size);

        // No chunk allocated: the whole range was never written.
        if (!chunkRef.ChunkIdx) {
            auto zero = TRcBuf::Uninitialized(selector.Size);
            memset(zero.GetDataMut(), 0, zero.size());
            TRope result(std::move(zero));
            std::vector<ui64> checksums;
            if (Config.EnableChecksums) {
                checksums.assign(selector.Size / IntegrityUnitSize, GetZeroBlockChecksum());
            }
            Counters.Interface.Read.Reply(true, selector.Size, 0);
            SendReply(*ev, std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                std::move(result), checksums));
            return;
        }

        if (Config.EnableChecksums) {
            const ui64 integrityOperationId = IntegrityManager->BeginChecksumRead(
                {creds.TabletId, selector.VChunkIndex}, selector.OffsetInBytes, selector.Size);
            const bool inserted = PendingChecksumReads.emplace(integrityOperationId,
                TPendingChecksumRead{std::unique_ptr<IEventHandle>(ev.Release())}).second;
            Y_ABORT_UNLESS(inserted);
            DrainIntegrityManager();
        } else {
            StartDDiskDataRead(std::unique_ptr<IEventHandle>(ev.Release()), {});
        }
    }

    void TDDiskActor::StartDDiskDataRead(std::unique_ptr<IEventHandle> ev,
            std::vector<ui64> checksums) {
        const auto& record = ev->Get<TEvRead>()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        TChunkRef& chunkRef = ChunkRefs.at(creds.TabletId).at(selector.VChunkIndex);

        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Read",
            NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span
            .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size);

        std::optional<TIntegrityManager::TReadPlan> plan;
        if (Config.EnableChecksums) {
            plan.emplace(IntegrityManager->MakeReadPlan({creds.TabletId, selector.VChunkIndex},
                selector.OffsetInBytes, selector.Size));
            if (plan->Kind == TIntegrityManager::TReadPlan::AllZero) {
                auto zero = TRcBuf::Uninitialized(selector.Size);
                memset(zero.GetDataMut(), 0, zero.size());
                TRope result(std::move(zero));
                Counters.Interface.Read.Reply(true, selector.Size, 0);
                span.End();
                SendReply(*ev, std::make_unique<TEvReadResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                    std::move(result), checksums));
                return;
            }
        }

        auto offset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TDDiskIoOp>(ev.get());
        auto* ddiskOp = static_cast<TDDiskIoOp*>(op.get());
        ddiskOp->SetChunkKey(creds.TabletId, selector.VChunkIndex);
        ddiskOp->SetReadChecksums(std::move(checksums));
        op->SetSpan(std::move(span));
        op->PrepareRead(selector.Size, offset, chunkRef.ChunkIdx, selector.OffsetInBytes);
        if (plan && plan->Kind == TIntegrityManager::TReadPlan::Mixed) {
            // The unused blocks are zero-filled right before the reply, on the uring I/O
            // thread, so the mask travels inside the op.
            op->SetReadUsedBlocksMask(std::move(plan->UsedBlocks));
        }

        ++chunkRef.InFlightDataIo;
        DirectUringOp(op);
    }

    void TDDiskActor::Handle(TEvPrivate::TEvDDiskIoResult::TPtr ev) {
        auto& msg = *ev->Get();
        Y_ABORT_UNLESS(msg.HasChunkKey);
        const auto tabletIt = ChunkRefs.find(msg.TabletId);
        Y_ABORT_UNLESS(tabletIt != ChunkRefs.end());
        const auto chunkIt = tabletIt->second.find(msg.VChunkIndex);
        Y_ABORT_UNLESS(chunkIt != tabletIt->second.end());
        Y_ABORT_UNLESS(chunkIt->second.InFlightDataIo > 0);
        --chunkIt->second.InFlightDataIo;

        auto status = msg.Status;
        TString errorMessage = std::move(msg.ErrorMessage);
        if (Y_UNLIKELY(IsBroken())) {
            status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
            errorMessage = GetBrokenReason();
        }

        if (msg.OperationType == NPDisk::TUringOperationBase::EWRITE && msg.IntegrityOperationId) {
            const auto pendingIt = PendingClientWrites.find(msg.IntegrityOperationId);
            Y_ABORT_UNLESS(pendingIt != PendingClientWrites.end());
            pendingIt->second.DataResult.emplace(TParkedWriteReply{
                .Status = status,
                .ErrorMessage = std::move(errorMessage),
                .OriginalRequester = msg.OriginalRequester,
                .InterconnectSession = msg.InterconnectSession,
                .Cookie = msg.Cookie,
                .Span = std::move(msg.Span),
                .TotalSize = msg.TotalSize,
                .RequestTimeMs = msg.RequestTimeMs,
                .TabletId = msg.TabletId,
                .VChunkIndex = msg.VChunkIndex,
            });
            MaybeFinishClientWrite(msg.IntegrityOperationId);
            return;
        }

        if (msg.OperationType == NPDisk::TUringOperationBase::EWRITE && msg.HasChunkKey) {
            const auto it = DataChunkAllocationsInFlight.find({msg.TabletId, msg.VChunkIndex});
            if (it != DataChunkAllocationsInFlight.end()) {
                it->second.ParkedWriteResults.push_back(TParkedWriteReply{
                    .Status = status,
                    .ErrorMessage = std::move(errorMessage),
                    .OriginalRequester = msg.OriginalRequester,
                    .InterconnectSession = msg.InterconnectSession,
                    .Cookie = msg.Cookie,
                    .Span = std::move(msg.Span),
                    .TotalSize = msg.TotalSize,
                    .RequestTimeMs = msg.RequestTimeMs,
                    .TabletId = msg.TabletId,
                    .VChunkIndex = msg.VChunkIndex,
                });
                return;
            }
        }

        const bool isOk = status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        std::optional<TString> errorReason;
        if (errorMessage) {
            errorReason.emplace(std::move(errorMessage));
        }

        std::unique_ptr<IEventBase> reply;
        switch (msg.OperationType) {
            case NPDisk::TUringOperationBase::EREAD:
                reply = std::make_unique<TEvReadResult>(
                    status, errorReason, isOk ? std::move(msg.Data) : TRope{},
                    isOk ? msg.Checksums : std::vector<ui64>{});
                Counters.Interface.Read.Reply(isOk, msg.TotalSize, msg.RequestTimeMs);
                break;
            case NPDisk::TUringOperationBase::EWRITE:
                reply = std::make_unique<TEvWriteResult>(status, errorReason);
                Counters.Interface.Write.Reply(isOk, msg.TotalSize, msg.RequestTimeMs);
                break;
            default:
                Y_ABORT("Unknown OperationType");
        }

        auto h = std::make_unique<IEventHandle>(msg.OriginalRequester, SelfId(), reply.release(),
            0, msg.Cookie, nullptr, msg.Span.GetTraceId());
        if (msg.InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, msg.InterconnectSession);
        }
        msg.Span.End();
        TActivationContext::Send(h.release());
    }

    void TDDiskActor::MaybeFinishClientWrite(ui64 operationId) {
        const auto it = PendingClientWrites.find(operationId);
        if (it == PendingClientWrites.end() || !it->second.DataResult
                || !it->second.IntegrityCompleted) {
            return;
        }

        TParkedWriteReply result = std::move(*it->second.DataResult);
        if (Y_UNLIKELY(IsBroken())) {
            result.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
            result.ErrorMessage = GetBrokenReason();
        } else if (it->second.IntegrityStatus == TIntegrityManager::EOperationStatus::Corrupted
                && result.Status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            result.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::CORRUPTED;
            result.ErrorMessage = std::move(it->second.IntegrityError);
        }
        PendingClientWrites.erase(it);
        ReleaseIntegrityExtentWrite(result.TabletId, result.VChunkIndex);
        FinishClientWrite(std::move(result));
    }

    void TDDiskActor::FinishClientWrite(TParkedWriteReply result) {
        const auto allocationIt = DataChunkAllocationsInFlight.find({
            result.TabletId, result.VChunkIndex});
        if (allocationIt != DataChunkAllocationsInFlight.end()) {
            allocationIt->second.ParkedWriteResults.push_back(std::move(result));
            return;
        }

        const bool isOk = result.Status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        std::optional<TString> errorReason;
        if (result.ErrorMessage) {
            errorReason.emplace(std::move(result.ErrorMessage));
        }
        auto reply = std::make_unique<TEvWriteResult>(result.Status, errorReason);
        Counters.Interface.Write.Reply(isOk, result.TotalSize, result.RequestTimeMs);
        auto h = std::make_unique<IEventHandle>(result.OriginalRequester, SelfId(), reply.release(),
            0, result.Cookie, nullptr, result.Span.GetTraceId());
        if (result.InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, result.InterconnectSession);
        }
        result.Span.End();
        TActivationContext::Send(h.release());
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkReadRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        YDB_LOG_DEBUG_COMP(BS_DDISK, "TDDiskActor::Handle(TEvChunkReadRawResult)",
            {"marker", "BSDD08"},
            {"DDiskId", DDiskId},
            {"msg", msg});

        auto it = ReadCallbacks.find(ev->Cookie);
        if (it == ReadCallbacks.end()) {
            Y_ABORT_UNLESS(IsBroken());
            return;
        }

        if (Y_UNLIKELY(IsBroken())) {
            std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
            ReadCallbacks.erase(it);
            op->SetResult(-EIO);
            op.release()->OnComplete(TActorContext::ActorSystem());
            return;
        }

        if (msg.Status != NKikimrProto::OK) {
            if (it->second.Op->IsCriticalDDiskIo()) {
                // Complete fallback integrity reads through the same path as an io_uring EIO.
                // TEvIntegrityIoResult will latch Broken and fail every joined client request.
                std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
                ReadCallbacks.erase(it);
                op->SetResult(-EIO);
                op.release()->OnComplete(TActorContext::ActorSystem());
                return;
            }
            if (!CheckPDiskReply(msg.Status, msg.ErrorReason, "Handle(TEvChunkReadRawResult)")) {
                return;
            }
        }

        std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
        ReadCallbacks.erase(it);

        // fill the op with result and finish via common completion path
        Y_DEBUG_ABORT_UNLESS(op->GetTotalSize() <= static_cast<ui64>(Max<i32>()));
        op->SetResult(static_cast<i32>(op->GetTotalSize()), std::move(msg.Data));

        op.release()->OnComplete(TActorContext::ActorSystem());
    }

    void TDDiskActor::DirectUringOpImpl(std::unique_ptr<TDirectIoOpBase>& op) {
#if defined(__linux__)
        Y_ABORT_UNLESS(UringRouter);

        // The router may complete the operation on its I/O thread before the
        // submission call returns. Transfer ownership and publish the running
        // counter before making the call, and do not touch rawOp after acceptance.
        TDirectIoOpBase* rawOp = op.release();
        Counters.DirectIO.RunningCount->Inc();

        // this is our main/regular path
        switch (rawOp->GetOperationType()) {
        case NPDisk::TUringOperationBase::EREAD:
            Y_ABORT_UNLESS(UringRouter->Read(rawOp),
                "live io_uring router rejected a read submission");
            break;
        case NPDisk::TUringOperationBase::EWRITE:
            Y_ABORT_UNLESS(UringRouter->Write(rawOp),
                "live io_uring router rejected a write submission");
            break;
        default:
            Y_ABORT("Unknown OperationType");
        }
#else
        Y_UNUSED(op);
        Y_ABORT("DirectUringOpImpl is only available on Linux");
#endif
    }

    void TDDiskActor::DirectUringOp(std::unique_ptr<TDirectIoOpBase>& op, bool isShort) {
        if (Y_UNLIKELY(IsBroken())) {
            if (isShort) {
                switch (op->GetOperationType()) {
                    case NPDisk::TUringOperationBase::EREAD:
                        Counters.DirectIO.Read.Done(op->GetTotalSize());
                        break;
                    case NPDisk::TUringOperationBase::EWRITE:
                        Counters.DirectIO.Write.Done(op->GetTotalSize());
                        break;
                    default:
                        Y_ABORT("Unknown OperationType");
                }
            }
            op->Reply(TActivationContext::ActorSystem(),
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason());
            op.reset();
            return;
        }

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
            DirectUringOpImpl(op);
            return;
        }
#endif

        Counters.DirectIO.RunningCount->Inc();

        // fallback path: either not linux or uring disabled / not available
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

#if defined(__linux__)
        if (Y_LIKELY(UringRouter)) {
            DirectUringOp(op, /*isShort=*/true);
            return;
        }

        switch (op->GetOperationType()) {
            case NPDisk::TUringOperationBase::EREAD:
                Counters.DirectIO.Read.Done(op->GetTotalSize());
                break;
            case NPDisk::TUringOperationBase::EWRITE:
                Counters.DirectIO.Write.Done(op->GetTotalSize());
                break;
            default:
                Y_ABORT("Unknown OperationType");
        }
        op->Reply(TActivationContext::ActorSystem(),
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
            "io_uring stopped before short-I/O retry");
        op.reset();
#else
        Y_UNUSED(op);
        Y_ABORT("TEvShortIO is only available with io_uring");
#endif
    }

    void TDDiskActor::HandleWakeup(TEvents::TEvWakeup::TPtr &ev) {
        switch (ev->Get()->Tag) {
            case EWakeupTag::WakeupUpdateFreeSpaceInfo: {
                UpdateFreeSpaceInfo();
                break;
            }
            case EWakeupTag::WakeupCollectPbStats: {
                CollectPbStatsSnapshot();
                break;
            }
            case EWakeupTag::WakeupProcessPersistentBufferBatchWrite: {
                ProcessPersistentBufferBatchWrite();
                break;
            }
            case EWakeupTag::WakeupProcessDeallocatePersistentBufferChunk: {
                ProcessDeallocatePersistentBufferChunk(true);
                break;
            }
            case EWakeupTag::WakeupFlushDeviceOverestimationSamples: {
                FlushDeviceOverestimationSamples();
                break;
            }
        }
    }

} // NKikimr::NDDisk
