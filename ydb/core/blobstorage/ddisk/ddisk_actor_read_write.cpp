#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <ydb/core/util/stlog.h>

#include <cerrno>
#include <util/generic/scope.h>

namespace NKikimr::NDDisk {

    using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

    TDDiskActor::TPendingIoOp::TPendingIoOp(std::unique_ptr<TDirectIoOpBase> op)
        : Op(std::move(op))
    {}

    TDDiskActor::TPendingIoOp::TPendingIoOp(TPendingIoOp&&) noexcept = default;
    TDDiskActor::TPendingIoOp& TDDiskActor::TPendingIoOp::operator=(TPendingIoOp&&) noexcept = default;
    TDDiskActor::TPendingIoOp::~TPendingIoOp() = default;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TDDiskActor::TBatchedIOAwaiter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    TDDiskActor::TBatchedIOAwaiter::TBatchedIOAwaiter(TDDiskActor& actor) noexcept
        : Actor(actor)
    {}

    // Out of line: the prepared operations are held by pointers to a type that is
    // only complete here.
    TDDiskActor::TBatchedIOAwaiter::~TBatchedIOAwaiter() = default;

    size_t TDDiskActor::TBatchedIOAwaiter::ReserveMetadataSlot() {
        const size_t index = MetadataResults.size();
        MetadataResults.emplace_back();
        return index;
    }

    void TDDiskActor::TBatchedIOAwaiter::Add(std::unique_ptr<TDDiskIoOp> op) {
        if (op) {
            Prepared.push_back(std::move(op));
        }
    }

    void TDDiskActor::TBatchedIOAwaiter::OnComplete(TIoCompletion&& completion,
            std::optional<size_t> metadataIndex) noexcept
    {
        // Only the last completion gets here with the bridge, and only once the guard of
        // Submit() has been released, so the bridge is always published by then.
        if (RecordCompletion(std::move(completion), metadataIndex)) {
            if (auto bridge = TakeBridge()) {
                bridge.resume();
            }
        }
    }

    bool TDDiskActor::TBatchedIOAwaiter::RecordCompletion(TIoCompletion&& completion,
            std::optional<size_t> metadataIndex) noexcept
    {
        if (metadataIndex) {
            MetadataResults[*metadataIndex] = std::move(completion);
        } else {
            DataResult.Status = completion.Status;
            DataResult.ErrorMessage = std::move(completion.ErrorMessage);
            DataResult.Data = std::move(completion.Data);
        }
        return Pending.fetch_sub(1, std::memory_order_acq_rel) == 1;
    }

    std::coroutine_handle<> TDDiskActor::TBatchedIOAwaiter::Submit(std::coroutine_handle<> bridge) noexcept {
        Y_ABORT_UNLESS(!Bridge.load(std::memory_order_relaxed));
        // The extra count is the submission guard: completions of already submitted
        // operations, even inline ones, cannot reach zero before it is released.
        Pending.fetch_add(static_cast<ui32>(Prepared.size()) + 1, std::memory_order_acq_rel);
        Bridge.store(bridge.address(), std::memory_order_release);
        // Bind late: an operation which was prepared but never submitted must not own
        // the batch that owns it.
        const auto self = shared_from_this();
        for (auto& prepared : Prepared) {
            prepared->SetCallback(self);
            std::unique_ptr<TDirectIoOpBase> op = std::move(prepared);
            Actor.DirectUringOp(op);
        }
        Prepared.clear();
        if (Pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // Everything completed while submitting: nobody else holds the bridge.
            Bridge.store(nullptr, std::memory_order_relaxed);
            return bridge;
        }
        return std::noop_coroutine();
    }

    void TDDiskActor::TBatchedIOAwaiter::AbandonBridge() noexcept {
        if (auto bridge = TakeBridge()) {
            bridge.destroy();
        }
    }

    void TDDiskActor::TBatchedIOAwaiter::ClearForReuse() noexcept {
        Y_DEBUG_ABORT_UNLESS(Prepared.empty() && !Pending.load(std::memory_order_relaxed)
            && !Bridge.load(std::memory_order_relaxed));
        DataResult = {};
        MetadataResults.clear();
    }

    std::shared_ptr<TDDiskActor::TBatchedIOAwaiter> TDDiskActor::AllocateBatchedIOAwaiter() {
        if (BatchedIOAwaiterPool.empty()) {
            return std::make_shared<TBatchedIOAwaiter>(*this);
        }
        auto batch = std::move(BatchedIOAwaiterPool.back());
        BatchedIOAwaiterPool.pop_back();
        return batch;
    }

    void TDDiskActor::ReturnBatchedIOAwaiter(std::shared_ptr<TBatchedIOAwaiter> batch) {
        // A device operation or the currently executing resume can still own the batch.
        // Leave that object untouched until its remaining owners release it.
        if (batch.use_count() != 1 || BatchedIOAwaiterPool.size() >= IoOpPoolCapacity) {
            return;
        }
        batch->ClearForReuse();
        BatchedIOAwaiterPool.push_back(std::move(batch));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // PDisk fallback submission
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Write
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TDDiskActor::Handle(TEvWrite::TPtr ev) {
        YDB_LOG_TRACE_COMP(BS_DDISK, "TDDiskActor::Handle(TEvWrite)",
            {"marker", "BSDD50"},
            {"DDiskId", DDiskId},
            {"sender", ev->Sender},
            {"cookie", ev->Cookie});

        TQueryCredentials creds;
        if (!CheckQueryImpl<false>(*ev, &Counters.Interface.Write, creds)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials originalCredentials(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const TWriteInstruction instr(record.GetInstruction());

        if (selector.Size > MaxWriteSize) {
            Counters.Interface.Write.Request(0);
            Counters.Interface.Write.Reply(false);
            SendReply(*ev, std::make_unique<TEvWriteResult>(TStatus::INCORRECT_REQUEST,
                "write size must not exceed 1 MiB"));
            return;
        }

        if (TabletChunkDeletionsInFlight.contains(creds.TabletId)) {
            Counters.Interface.Write.Request(0);
            Counters.Interface.Write.Reply(false);
            SendReply(*ev, std::make_unique<TEvWriteResult>(
                TStatus::BUSY,
                "tablet chunk deletion is in flight"));
            return;
        }

        if (!ev->Get()->PayloadAlignmentChecked && instr.PayloadId) {
            ev->Get()->PayloadAlignmentChecked = true;
            const TRope& data = ev->Get()->GetPayload(*instr.PayloadId);
            const auto dataIter = data.Begin();
            if (dataIter.ContiguousSize() != data.size() ||
                    reinterpret_cast<uintptr_t>(dataIter.ContiguousData()) % DiskFormat->SectorSize != 0) {
                Counters.Interface.UnalignedWritePayloads->Inc();
            }
        }

        if (selector.OffsetInBytes % IntegrityUnitSize || selector.Size % IntegrityUnitSize) {
            Counters.Interface.Write.Request(0);
            Counters.Interface.Write.Reply(false);
            SendReply(*ev, std::make_unique<TEvWriteResult>(
                TStatus::INCORRECT_REQUEST,
                "write offset and size must be aligned to 4 KiB"));
            return;
        }

        if (Config.EnableChecksums) {
            if (!HasRequiredBlockChecksums(record.ChecksumsSize(), selector.OffsetInBytes, selector.Size)) {
                if (record.ChecksumsSize() == 0) {
                    Counters.Checksums.WritesWithoutChecksums->Inc();
                }
                Counters.Interface.Write.Request(0);
                Counters.Interface.Write.Reply(false);
                SendReply(*ev, std::make_unique<TEvWriteResult>(
                    TStatus::INCORRECT_REQUEST,
                    "one checksum per aligned 4 KiB block is required"));
                return;
            }

            Y_ABORT_UNLESS(instr.PayloadId, "TEvWrite without a payload, but with checksums");

            if (Config.CheckChecksumBeforeWrite) {
                const TRope& payload = ev->Get()->GetPayload(*instr.PayloadId);
                if (const auto result = ValidatePayloadChecksums(record, payload)) {
                    const bool isCorrupted = result->Status == TStatus::CORRUPTED;
                    Counters.Interface.Write.Request(0);
                    Counters.Interface.Write.Reply(false);
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
        }

        auto& tablet = Tablets[creds.TabletId];
        Counters.Interface.Write.Request(selector.Size);
        CountTabletIo(creds.TabletId, &tablet.Stats, ETabletOperation::Write, 1, selector.Size);

        TDataWrite write{
            .OriginalCredentials = originalCredentials,
            .ResolvedCredentials = creds,
            .Selector = selector,
            .Data = instr.PayloadId ? ev->Get()->GetPayload(*instr.PayloadId) : TRope{},
            .Checksums = {record.GetChecksums().begin(), record.GetChecksums().end()},
            .Reply = {ev->Sender, ev->InterconnectSession, ev->Cookie},
            .StartTs = HPNow(),
        };

        Y_ABORT_UNLESS(write.Data.size() == selector.Size);

        write.Span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Write",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(write.Span);
        write.Span
            .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size);

        // The write owns every input it needs beyond this turn, including the original token.
        ev.Reset(nullptr);

        // THashMap keeps references stable; the pin forbids deleting this entry while the
        // write coroutine owns it. The coroutine releases it after replying.
        auto& chunkRef = tablet.ChunkRefs[selector.VChunkIndex];
        ++chunkRef.ChunkRefPins;
        ExecuteDataWrite(std::move(write), &chunkRef);
    }

    bool TDDiskActor::WriteSessionMatches(
            const TQueryCredentials& original, const TQueryCredentials& resolved) const
    {
        TQueryCredentials current;
        return ResolveConnection(original, &current) == EConnectionResolution::Resolved
            && current.TabletId == resolved.TabletId
            && current.Generation == resolved.Generation
            && current.DDiskSessionSeqNo == resolved.DDiskSessionSeqNo
            && current.DirectBlockGroupIndex == resolved.DirectBlockGroupIndex
            && current.DDiskInstanceGuid == resolved.DDiskInstanceGuid;
    }

    std::unique_ptr<TDDiskActor::TDDiskIoOp> TDDiskActor::PrepareDataWrite(TChunkIdx chunk,
            const TBlockSelector& selector, TRope data)
    {
        auto write = AllocateOp<TDDiskIoOp>();
        write->PrepareWrite(std::move(data), DiskFormat->Offset(chunk, 0, selector.OffsetInBytes),
            chunk, selector.OffsetInBytes);
        return write;
    }

    void TDDiskActor::CompleteMetadataWrite(TIntegrityManager::TWriteOperation& operation,
            const std::shared_ptr<TIntegrityManager::TMetadataWrite>& context, const TIoCompletion& completion)
    {
        if (completion.Status != TStatus::OK && completion.Status != TStatus::CORRUPTED
                && completion.Status != TStatus::SESSION_MISMATCH && !Stopping) {
            EnterBroken(completion.ErrorMessage);
        }
        IntegrityManager->CompleteMetadataWrite(context,
            completion.Status == TStatus::OK && !IsBroken());
        CountIntegrityResult(*operation.GetResult());
        IntegrityManager->NotifyCompleted();
    }

    void TDDiskActor::ExecuteDataWrite(TDataWrite write, TChunkRef* chunk) {
        using EOperationStatus = TIntegrityManager::EOperationStatus;

        TSyncInFlight* const sync = write.Sync.get();
        const ui64 tabletId = write.ResolvedCredentials.TabletId;
        const ui64 vChunkIndex = write.Selector.VChunkIndex;
        const TStringBuf staleSessionReason = sync
            ? "session replaced while sync was waiting"
            : "session replaced while write was waiting";

        Y_DEFER {
            if (chunk) {
                --chunk->ChunkRefPins;
            }
            if (sync) {
                SyncSourceCookies.erase(sync->SourceCookie);
                SyncsInFlight.erase(sync->Id);
                *Counters.Interface.Sync.BytesInFlight -= sync->RequestedBytes;
            }
        };

        TDataRequestGuard requestGuard(*this);
        TStatus::E writeStatus = TStatus::ERROR;
        TString writeError;
        bool dataAdmitted = false;

        auto valid = [&](TStatus::E& status, TString& error) {
            if (Stopping || IsBroken()) {
                status = IsBroken() ? TStatus::ERROR : TStatus::SESSION_MISMATCH;
                error = IsBroken() ? GetBrokenReason() : TString(StoppingReason);
                return false;
            }
            if (!WriteSessionMatches(write.OriginalCredentials, write.ResolvedCredentials)) {
                status = TStatus::SESSION_MISMATCH;
                error = TString(staleSessionReason);
                return false;
            }
            return true;
        };

        // A Write is a single piece. A Sync processes its inputs in order, each in aligned pieces
        // of at most SyncPieceSize in increasing offset order. The next source read starts only
        // after the destination operations of the current piece retire.
        size_t inputIndex = 0;
        ui32 offset = 0;
        while (!sync || inputIndex < sync->Requests.size()) {
            TSyncReadRequest* input = sync ? &sync->Requests[inputIndex] : nullptr;
            if (input && (input->Status != TStatus::OK || offset >= input->Selector.Size
                    || Stopping || IsBroken())) {
                ++inputIndex;
                offset = 0;
                continue;
            }
            TStatus::E& status = input ? input->Status : writeStatus;
            TString& error = input ? input->ErrorReason : writeError;

            do {
                if (input) {
                    write.Selector = input->Selector;
                    write.Selector.OffsetInBytes += offset;
                    write.Selector.Size = Min(SyncPieceSize, input->Selector.Size - offset);
                    SendSyncSourceRead(write, *input);

                    // Must be a loop: Changed also fires on chunk commits of other writers, so a
                    // wakeup does not imply a source result. The result itself is retained
                    // independently of this non-sticky notification.
                    while (!sync->SourceResult && !Stopping && !IsBroken()) {
                        auto waiter = sync->Changed.Wait();
                        co_await NonCancellable(waiter);
                    }

                    SyncSourceCookies.erase(std::exchange(sync->SourceCookie, 0));
                    if (!sync->SourceResult) {
                        valid(status, error);
                        break;
                    }
                    if (!AcceptSyncSource(write, *input)) {
                        break;
                    }
                }

                if (!valid(status, error)) {
                    break;
                }

                if (!chunk) {
                    chunk = &Tablets[tabletId].ChunkRefs[vChunkIndex];
                    ++chunk->ChunkRefPins; // Y_DEFER above handles the decrement
                }

                // chunk allocation here is getting reserve from PDisk (might be already gotten)
                // and optionally formatting data chunk / allocating extents and formatting them
                if (!chunk->ChunkIdx && !chunk->AllocationPending) {
                    IssueChunkAllocation(tabletId, vChunkIndex);
                }

                if (chunk->AllocationPending && !Stopping && !IsBroken()) {
                    auto waiter = chunk->AllocationReady.Wait();
                    co_await NonCancellable(waiter);
                }

                if (!valid(status, error)) {
                    break;
                }

                if (!chunk->ChunkIdx) {
                    status = TStatus::ERROR;
                    error = "destination chunk allocation failed";
                    break;
                }

                // Each step below records its failure as it happens; OK means none so far.
                // We have chunk (and corresponding integrity chunk) and can proceed with data and metadata
                const TChunkIdx chunkIdx = chunk->ChunkIdx;
                TStatus::E pieceStatus = TStatus::OK;
                TString pieceError;

                bool admitted = false;
                auto batch = AllocateBatchedIOAwaiter();
                std::optional<TIntegrityManager::TWriteOperation> metadata;
                std::shared_ptr<TIntegrityManager::TMetadataWrite> context;

                // Set when the metadata leg ends before any device write, with the completion
                // that CompleteMetadataWrite should account for.
                std::optional<TIoCompletion> metadataFailure;

                if (Config.EnableChecksums) {
                    // No data is admitted before this root owns every metadata pair of the range, so
                    // waiting for neighboring writers never holds accepted device buffers.
                    metadata.emplace(IntegrityManager->PrepareWrite(
                        {tabletId, vChunkIndex},
                        write.Selector.OffsetInBytes,
                        write.Selector.Size));
                    while (!metadata->IsReady()) {
                        auto waiter = metadata->WaitChanged();
                        co_await NonCancellable(waiter);
                    }
                    if (const auto* outcome = metadata->GetResult()) {
                        CountIntegrityResult(*outcome);
                        pieceStatus = outcome->Status == EOperationStatus::Corrupted
                            ? TStatus::CORRUPTED : TStatus::SESSION_MISMATCH;
                        pieceError = outcome->ErrorReason;
                    } else if (!valid(pieceStatus, pieceError)) {
                        metadata->Cancel();
                    } else {
                        // From here the integrity update is committed to: it completes even if the
                        // session changes while a cold pair is loaded.
                        context = IntegrityManager->PrepareMetadataWrite(*metadata, write.Checksums);
                        Counters.Checksums.ChecksumWrites->Inc();
                    }
                }

                if (context) {
                    if (context->Result.Status != EOperationStatus::Ok) {
                        metadataFailure.emplace();
                        metadataFailure->Status = TStatus::CORRUPTED;
                        metadataFailure->ErrorMessage = context->Result.ErrorReason;
                    } else if (context->ReadSize) {
                        // Cold pair: nobody has the image, so read, merge and rewrite it here.
                        Counters.Checksums.MetadataReads->Inc();
                        const TIntegrityManager::TMetadataRead read{0, context->ChunkIdx, context->ReadOffset,
                            context->ReadSize};
                        batch->Add(PrepareMetadataRead(*batch, read));
                        co_await batch->Wait();
                        TIoCompletion load = std::move(batch->MetadataResults[0]);
                        batch->ClearForReuse();
                        if (load.Status != TStatus::OK) {
                            metadataFailure = std::move(load);
                        } else if (Stopping || IsBroken()) {
                            metadataFailure.emplace();
                            metadataFailure->Status = Stopping ? TStatus::SESSION_MISMATCH : TStatus::ERROR;
                            metadataFailure->ErrorMessage = Stopping ? TString(StoppingReason) : GetBrokenReason();
                        } else if (!context->Transform(load.Data)) {
                            metadataFailure.emplace();
                            metadataFailure->Status = TStatus::CORRUPTED;
                            metadataFailure->ErrorMessage = context->Result.ErrorReason;
                        }
                    }
                }

                // Both writes go out together, and both retire before the piece completes.
                std::optional<size_t> metadataSlot;
                if (!Config.EnableChecksums || (context && !metadataFailure)) {
                    admitted = true;
                    dataAdmitted = true;
                    batch->Add(PrepareDataWrite(chunkIdx, write.Selector, std::move(write.Data)));
                    if (context) {
                        metadataSlot = batch->ReserveMetadataSlot();
                        batch->Add(PrepareCriticalWrite(context->ChunkIdx, context->WriteOffset,
                            context->WriteImage, metadataSlot));
                    }
                    co_await batch->Wait();
                }

                if (context) {
                    CompleteMetadataWrite(*metadata, context,
                        metadataFailure ? *metadataFailure : batch->MetadataResults[*metadataSlot]);
                    const auto& outcome = *metadata->GetResult();
                    if (outcome.Status != EOperationStatus::Ok
                            && (pieceStatus == TStatus::OK || outcome.Status == EOperationStatus::Corrupted)) {
                        pieceStatus = outcome.Status == EOperationStatus::Corrupted
                            ? TStatus::CORRUPTED : TStatus::SESSION_MISMATCH;
                        pieceError = outcome.ErrorReason;
                    }
                }

                if (admitted && batch->DataResult.Status != TStatus::OK && pieceStatus != TStatus::CORRUPTED) {
                    pieceStatus = batch->DataResult.Status;
                    pieceError = batch->DataResult.ErrorMessage;
                }
                status = pieceStatus;
                error = std::move(pieceError);
                ReturnBatchedIOAwaiter(std::move(batch));
                metadata.reset();

                if (Config.EnableChecksums) {
                    IntegrityManager->NotifyCompleted();
                }

                if (admitted && input) {
                    input->Retired += write.Selector.Size;
                }
            } while (false);

            if (!sync) {
                break;
            }

            offset += write.Selector.Size;
        }

        // Admitted data is acknowledged only after the chunk mapping commits. A Sync waits even
        // after a source failure, for any allocation already admitted for its chunk.
        if (sync || dataAdmitted) {
            auto& commitChanged = sync ? sync->Changed : chunk->CommitReady;
            while (!IsChunkCommitted(tabletId, vChunkIndex) && !Stopping && !IsBroken()) {
                auto waiter = commitChanged.Wait();
                co_await NonCancellable(waiter);
            }
        }

        if (sync) {
            FinishSync(write);
        } else {
            if (dataAdmitted && !IsChunkCommitted(tabletId, vChunkIndex)) {
                writeStatus = TStatus::SESSION_MISMATCH;
                writeError = TString(StoppingReason);
            }
            FinishDDiskWrite(write, writeStatus, std::move(writeError));
        }

        requestGuard.Release();
        if (Stopping && !GetDirectIoInflight()) {
            FinishStopping();
        }
    }

    void TDDiskActor::FinishDDiskWrite(TDataWrite& write, TStatus::E status, TString error) {
        if (Y_UNLIKELY(IsBroken())) {
            status = TStatus::ERROR;
            error = GetBrokenReason();
        }
        const bool ok = status == TStatus::OK;
        auto reply = std::make_unique<TEvWriteResult>(status,
            error ? std::optional<TString>(std::move(error)) : std::nullopt);
        Counters.Interface.Write.Reply(ok, write.Selector.Size,
            HPMilliSecondsFloat(HPNow() - write.StartTs));
        auto h = std::make_unique<IEventHandle>(write.Reply.OriginalRequester, SelfId(), reply.release(),
            0, write.Reply.Cookie, nullptr, write.Span.GetTraceId());
        if (write.Reply.InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, write.Reply.InterconnectSession);
        }
        write.Span.End();
        TActivationContext::Send(h.release());
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
            op.release()->OnComplete(TActivationContext::ActorSystem());
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
                op.release()->OnComplete(TActivationContext::ActorSystem());
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

        op.release()->OnComplete(TActivationContext::ActorSystem());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Read
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TDDiskActor::Handle(TEvRead::TPtr ev) {
        YDB_LOG_TRACE_COMP(BS_DDISK, "TDDiskActor::Handle(TEvRead)",
            {"marker", "BSDD21"},
            {"DDiskId", DDiskId},
            {"msg", ev->Get()->Record});

        TQueryCredentials creds;
        if (!CheckQuery(*ev, &Counters.Interface.Read, creds)) {
            return;
        }

        const TBlockSelector selector(ev->Get()->Record.GetSelector());

        if (selector.OffsetInBytes % IntegrityUnitSize != 0
                || selector.Size % IntegrityUnitSize != 0) {
            Counters.Interface.Read.Request(0);
            Counters.Interface.Read.Reply(false);
            SendReply(*ev, std::make_unique<TEvReadResult>(
                TStatus::INCORRECT_REQUEST,
                "read offset and size must be aligned to the 4 KiB integrity unit"));
            return;
        }

        if (TabletChunkDeletionsInFlight.contains(creds.TabletId)) {
            Counters.Interface.Read.Request(0);
            Counters.Interface.Read.Reply(false);
            SendReply(*ev, std::make_unique<TEvReadResult>(
                TStatus::BUSY,
                "tablet chunk deletion is in flight"));
            return;
        }

        // A read never waits for chunk allocation. Until placement publishes a physical chunk -
        // including while another request's allocation for this virtual chunk is still pending -
        // the range was never written and reads as zeroes. No write can have completed into an
        // unpublished chunk, because every write awaits that publication first.
        auto& tablet = Tablets[creds.TabletId];
        TChunkRef* publishedChunk = nullptr;
        if (const auto chunkIt = tablet.ChunkRefs.find(selector.VChunkIndex);
                chunkIt != tablet.ChunkRefs.end() && chunkIt->second.ChunkIdx
                && !chunkIt->second.AllocationPending) {
            publishedChunk = &chunkIt->second;
        }

        Counters.Interface.Read.Request(selector.Size);
        CountTabletIo(creds.TabletId, &tablet.Stats, ETabletOperation::Read, 1, selector.Size);

        if (!publishedChunk) {
            auto zero = TRcBuf::Uninitialized(selector.Size);
            memset(zero.GetDataMut(), 0, zero.size());
            TRope result(std::move(zero));
            auto reply = std::make_unique<TEvReadResult>(
                TStatus::OK, std::nullopt, std::move(result));
            if (Config.EnableChecksums) {
                const ui64 checksum = GetZeroBlockChecksum();
                const ui32 blocks = selector.Size / IntegrityUnitSize;
                reply->Record.MutableChecksums()->Reserve(static_cast<int>(blocks));
                for (ui32 block = 0; block < blocks; ++block) {
                    reply->Record.AddChecksums(checksum);
                }
            }
            Counters.Interface.Read.Reply(true, selector.Size, 0);
            SendReply(*ev, std::move(reply));
            return;
        }

        TDataRead read{
            .ResolvedCredentials = creds,
            .Selector = selector,
            .Reply = {ev->Sender, ev->InterconnectSession, ev->Cookie},
            .StartTs = HPNow(),
        };

        read.Span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Read",
            NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(read.Span);
        read.Span.Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes).Attribute("size", selector.Size);

        // The read owns every input it needs beyond this turn, including the reply route.
        ev.Reset(nullptr);

        ExecuteDataRead(std::move(read), publishedChunk);
    }

    void TDDiskActor::ExecuteDataRead(TDataRead read, TChunkRef* pinnedChunk) {
        ++pinnedChunk->ChunkRefPins;
        Y_DEFER { --pinnedChunk->ChunkRefPins; };

        auto batch = AllocateBatchedIOAwaiter();
        batch->DataResult.TotalSize = read.Selector.Size;

        TDataRequestGuard requestGuard(*this);
        bool notifyAfterReply = false;

        do {
            // easy case: we need just data block
            if (!Config.EnableChecksums) {
                batch->Add(PrepareDataRead(*batch, pinnedChunk->ChunkIdx, read.Selector));
                co_await batch->Wait();
                break;
            }

            auto preparation = IntegrityManager->PrepareRead(
                {read.ResolvedCredentials.TabletId, read.Selector.VChunkIndex},
                read.Selector.OffsetInBytes,
                read.Selector.Size);

            // cache hit: we have integrity block and don't need to read it.
            // preparation.Warm keeps a copy of needed checksums and holes position, so that
            // cache eviction or neighboring metadata writes can proceed during the data wait
            if (preparation.Warm) {
                if (preparation.Warm->Status == TIntegrityManager::EOperationStatus::Ok
                        && preparation.Warm->ReadPlan.Kind != TIntegrityManager::TReadPlan::AllZero) {
                    batch->Add(PrepareDataRead(*batch, pinnedChunk->ChunkIdx, read.Selector));
                    co_await batch->Wait();
                }
                ApplyDDiskReadMetadata(batch->DataResult, std::move(*preparation.Warm));
                break;
            }

            // cache miss: we need to either read metadata on our own or wait for previous reader
            // to finish the read

            // The previous read will handle metadata read for us, we just
            // need to read our data and then wait for metadata to be read.
            if (preparation.MetadataReads.empty()) {
                batch->Add(PrepareDataRead(*batch, pinnedChunk->ChunkIdx, read.Selector));
                co_await batch->Wait();

                // Usually metadata will be ready for us before our read completes,
                // and waiting for metadata should be free.
                if (!preparation.Pending.IsDone()) {
                    auto waiter = preparation.Pending.WaitChanged();
                    co_await NonCancellable(waiter);
                }
                ApplyDDiskReadMetadata(batch->DataResult, *preparation.Pending.GetResult());
                break;
            }

            // One metadata read covers the claimed pairs. A range that crosses a
            // ping-pong boundary is still one contiguous I/O; the manager splits it.
            Y_ABORT_UNLESS(preparation.MetadataReads.size() == 1);
            Counters.Checksums.MetadataReads->Inc();

            // for large reads we want to read metadata first for the case when there are
            // zero holes (epecially good if there is a single big zero hole)
            const bool metadataFirst = read.Selector.Size >= MetadataFirstReadThreshold;
            if (!metadataFirst) {
                batch->Add(PrepareDataRead(*batch, pinnedChunk->ChunkIdx, read.Selector));
            }
            batch->Add(PrepareMetadataRead(*batch, preparation.MetadataReads[0]));

            co_await batch->Wait();

            CompleteMetadataReads(preparation.MetadataReads, batch->MetadataResults);
            batch->MetadataResults.clear();
            if (metadataFirst || !preparation.Pending.IsDone()) {
                // Publish our owned loads and release their waiters before suspending
                // on another request's load or starting the large data read.
                IntegrityManager->NotifyCompleted();
                if (!preparation.Pending.IsDone()) {
                    auto waiter = preparation.Pending.WaitChanged();
                    co_await NonCancellable(waiter);
                }
            } else {
                // An initiating small read finishes before its metadata followers run.
                notifyAfterReply = true;
            }

            const auto& metadata = *preparation.Pending.GetResult();
            if (metadataFirst && metadata.Status == TIntegrityManager::EOperationStatus::Ok
                    && metadata.ReadPlan.Kind != TIntegrityManager::TReadPlan::AllZero) {
                batch->Add(PrepareDataRead(*batch, pinnedChunk->ChunkIdx, read.Selector));
                co_await batch->Wait();
            }
            ApplyDDiskReadMetadata(batch->DataResult, metadata);
        } while (false);

        FinishDDiskRead(read, batch->DataResult);

        // we were the one who read metadata, notify readers who is waiting for us
        if (notifyAfterReply) {
            IntegrityManager->NotifyCompleted();
        }

        ReturnBatchedIOAwaiter(std::move(batch));
        requestGuard.Release();

        if (Stopping && !GetDirectIoInflight()) {
            FinishStopping();
        }
    }

    std::unique_ptr<TDDiskActor::TDDiskIoOp> TDDiskActor::PrepareDataRead(TBatchedIOAwaiter& batch,
            TChunkIdx chunkIdx, const TBlockSelector& selector)
    {
        if (Stopping || IsBroken()) {
            batch.DataResult.Status = IsBroken() ? TStatus::ERROR : TStatus::SESSION_MISMATCH;
            batch.DataResult.ErrorMessage = IsBroken() ? GetBrokenReason() : TString(StoppingReason);
            return nullptr;
        }
        // DirectUringOp always produces exactly one completion, inline when the disk is broken.
        auto readOp = AllocateOp<TDDiskIoOp>();
        readOp->PrepareRead(selector.Size, DiskFormat->Offset(chunkIdx, 0, selector.OffsetInBytes),
            chunkIdx, selector.OffsetInBytes);
        return readOp;
    }

    std::unique_ptr<TDDiskActor::TDDiskIoOp> TDDiskActor::PrepareMetadataRead(TBatchedIOAwaiter& batch,
            const TIntegrityManager::TMetadataRead& read)
    {
        const size_t index = batch.ReserveMetadataSlot();
        if (Stopping) {
            auto& result = batch.MetadataResults[index];
            result.Status = TStatus::SESSION_MISMATCH;
            result.ErrorMessage = TString(StoppingReason);
            return nullptr;
        }
        auto metadataOp = AllocateOp<TDDiskIoOp>();
        metadataOp->SetCritical();
        metadataOp->SetMetadataIndex(index);
        metadataOp->PrepareRead(read.Size, DiskFormat->Offset(read.ChunkIdx, 0, read.OffsetInBytes),
            read.ChunkIdx, read.OffsetInBytes);
        return metadataOp;
    }

    void TDDiskActor::CompleteMetadataReads(TConstArrayRef<TIntegrityManager::TMetadataRead> reads,
            TArrayRef<TIoCompletion> completions)
    {
        Y_ABORT_UNLESS(reads.size() == completions.size());
        if (reads.empty()) {
            return;
        }
        absl::InlinedVector<TIntegrityManager::TMetadataReadResult, 1> results(reads.size());
        bool failed = false;
        TString error;
        for (size_t i = 0; i < reads.size(); ++i) {
            auto& result = results[i];
            auto& completion = completions[i];
            result.Id = reads[i].Id;
            result.Result.Ok = completion.Status == TStatus::OK;
            if (result.Result.Ok) {
                result.Result.Data = std::move(completion.Data);
            } else {
                failed = true;
                if (!error) {
                    error = std::move(completion.ErrorMessage);
                }
            }
        }
        // Latch the failure before handing the images over: CompleteMetadataReads resolves joined
        // reads, and none of them may observe a healthy disk after a critical load failed.
        if (failed && !Stopping) {
            EnterBroken(std::move(error));
        }
        IntegrityManager->CompleteMetadataReads(results);
    }

    void TDDiskActor::ApplyDDiskReadMetadata(TDDiskReadResult& result,
            TIntegrityManager::TOperationResult&& metadata)
    {
        ApplyDDiskReadMetadataImpl(result, std::move(metadata));
    }

    void TDDiskActor::ApplyDDiskReadMetadata(TDDiskReadResult& result,
            const TIntegrityManager::TOperationResult& metadata)
    {
        ApplyDDiskReadMetadataImpl(result, metadata);
    }

    template<class TMetadata>
    void TDDiskActor::ApplyDDiskReadMetadataImpl(TDDiskReadResult& result,
            TMetadata&& metadata)
    {
        CountIntegrityResult(metadata);
        if (metadata.Status != TIntegrityManager::EOperationStatus::Ok) {
            result.Status = metadata.Status == TIntegrityManager::EOperationStatus::Corrupted
                ? TStatus::CORRUPTED
                : TStatus::SESSION_MISMATCH;
            result.ErrorMessage = std::forward<TMetadata>(metadata).ErrorReason;
            return;
        }
        result.Checksums = std::forward<TMetadata>(metadata).Checksums;
        if (metadata.ReadPlan.Kind == TIntegrityManager::TReadPlan::AllZero) {
            auto zero = TRcBuf::Uninitialized(result.TotalSize);
            memset(zero.GetDataMut(), 0, zero.size());
            result.Data = TReadPayload(std::move(zero));
            result.Status = TStatus::OK;
            result.ErrorMessage.clear();
        } else if (result.Status == TStatus::OK
                && metadata.ReadPlan.Kind == TIntegrityManager::TReadPlan::Mixed) {
            auto data = result.Data.MutableSpan();
            for (size_t i = 0; i < data.size() / IntegrityUnitSize; ++i) {
                if (!metadata.ReadPlan.UsedBlocks.Get(i)) {
                    memset(data.data() + i * IntegrityUnitSize, 0, IntegrityUnitSize);
                }
            }
        }
    }

    void TDDiskActor::FinishDDiskRead(TDataRead& read, TDDiskReadResult& result) {
        auto status = result.Status;
        TString error = std::move(result.ErrorMessage);
        if (IsBroken()) {
            status = TStatus::ERROR;
            error = GetBrokenReason();
        }
        TRope data = std::move(result.Data).IntoRope();
        if (status == TStatus::OK
                && Config.EnableChecksums && Config.CheckChecksumWhenRead && !result.Checksums.empty()) {
            if (const auto failure = ValidatePayloadChecksums(result.Checksums, data)) {
                status = failure->Status;
                error = failure->ErrorReason;
                if (status == TStatus::CORRUPTED) {
                    Counters.Checksums.ChecksumMismatch->Inc();
                }
                YDB_LOG_ERROR_COMP(NKikimrServices::BS_DDISK, "DDisk read checksum validation failed",
                    {"DDiskId", DDiskId}, {"tabletId", read.ResolvedCredentials.TabletId},
                    {"vChunkIndex", read.Selector.VChunkIndex}, {"reason", error});
            }
        }
        const bool ok = status == TStatus::OK;
        auto reply = std::make_unique<TEvReadResult>(status,
            error ? std::optional<TString>(std::move(error)) : std::nullopt,
            ok ? std::move(data) : TRope{}, ok ? TConstArrayRef<ui64>(result.Checksums) : TConstArrayRef<ui64>{});
        Counters.Interface.Read.Reply(ok, result.TotalSize, HPMilliSecondsFloat(HPNow() - read.StartTs));
        auto handle = std::make_unique<IEventHandle>(read.Reply.OriginalRequester, SelfId(), reply.release(),
            0, read.Reply.Cookie, nullptr, read.Span.GetTraceId());
        if (read.Reply.InterconnectSession) {
            handle->Rewrite(TEvInterconnect::EvForward, read.Reply.InterconnectSession);
        }
        read.Span.End();
        TActivationContext::Send(handle.release());
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
            op.release()->OnComplete(TActivationContext::ActorSystem());
            return;
        }

        if (msg.Status != NKikimrProto::OK) {
            // A metadata read matches a standalone data read on explicit session loss:
            // stopping cancels it instead of latching Broken.
            const bool sessionLost = it->second.Op->IsCriticalDDiskIo()
                && (msg.Status == NKikimrProto::INVALID_OWNER || msg.Status == NKikimrProto::INVALID_ROUND);
            if (!sessionLost && (it->second.Op->IsCriticalDDiskIo() || it->second.Op->IsRestoreIo())) {
                // Complete fallback integrity and PB restore reads through the same path as an io_uring EIO
                // so that Broken is latched before any joined client request can be answered.
                std::unique_ptr<TDirectIoOpBase> op = std::move(it->second.Op);
                ReadCallbacks.erase(it);
                op->SetResult(-EIO);
                op.release()->OnComplete(TActivationContext::ActorSystem());
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

        op.release()->OnComplete(TActivationContext::ActorSystem());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Submission and retries
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TDDiskActor::DirectUringOpImpl(std::unique_ptr<TDirectIoOpBase>& op) {
#if defined(__linux__)
        Y_ABORT_UNLESS(UringRouter);

        // The router may complete the operation on its I/O thread before the
        // submission call returns. Transfer ownership and publish the running
        // counter before making the call, and do not touch rawOp after acceptance.
        TDirectIoOpBase* rawOp = op.release();
        Counters.DirectIO.RunningCount->Inc();
        DirectIoState.fetch_add(1, std::memory_order_relaxed);

        bool accepted = false;
        switch (rawOp->GetOperationType()) {
        case NPDisk::TUringOperationBase::EREAD:
            accepted = UringRouter->Read(rawOp);
            break;
        case NPDisk::TUringOperationBase::EWRITE:
            accepted = UringRouter->Write(rawOp);
            break;
        default:
            Y_ABORT("Unknown OperationType");
        }

        if (Y_UNLIKELY(!accepted)) {
            // StopAsync() makes rejection expected while PDisk is shutting
            // down. Submit() did not take ownership, so restore it and fail on
            // the actor thread; OnDrop() is reserved for accepted operations
            // and would violate the I/O-thread producer side of the op pool.
            op.reset(rawOp);
            FailDirectIoOp(std::move(op), "io_uring router stopped before submission");
            Send(SelfId(), new TEvPrivate::TEvBeginStopping);
        }
#else
        Y_UNUSED(op);
        Y_ABORT("DirectUringOpImpl is only available on Linux");
#endif
    }

    void TDDiskActor::DirectUringOp(std::unique_ptr<TDirectIoOpBase>& op, bool isRetry) {
        Y_ABORT_UNLESS(!Stopping);
        if (Y_UNLIKELY(IsBroken())) {
            if (isRetry) {
                op->GetCounters().Done(op->GetTotalSize());
            }
            op->Reply(TActivationContext::ActorSystem(),
                TStatus::ERROR, GetBrokenReason());
            op.reset();
            return;
        }

        if (Y_LIKELY(!isRetry)) {
            op->GetCounters().Request(op->GetTotalSize());
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

    TDDiskActor::TEvPrivate::TEvRetryIO::TEvRetryIO(std::unique_ptr<TDirectIoOpBase> op)
        : Op(std::move(op))
    {}

    TDDiskActor::TEvPrivate::TEvRetryIO::~TEvRetryIO() = default;

    void TDDiskActor::CancelPendingIo(std::unique_ptr<TDirectIoOpBase> op) {
        op->GetCounters().Done(op->GetTotalSize());
        op->Reply(TActivationContext::ActorSystem(), Stopping ? TStatus::SESSION_MISMATCH : TStatus::ERROR,
            Stopping ? TString(StoppingReason) : GetBrokenReason());
        // This is the actor thread, not the SPSC return-pool producer.
    }

    void TDDiskActor::CancelRetries() {
        while (!DelayedRetries.empty()) {
            auto it = DelayedRetries.begin();
            auto op = std::move(it->second.Op);
            DelayedRetries.erase(it);
            CancelPendingIo(std::move(op));
        }
    }

    void TDDiskActor::HandleRetryIO(TEvPrivate::TEvRetryIO::TPtr ev) {
        auto op = std::move(ev->Get()->Op);
        if (Stopping || IsBroken()) {
            CancelPendingIo(std::move(op));
            return;
        }
        Y_ABORT_UNLESS(op->RetryCount && op->RetryCount <= TDirectIoOpBase::MaxResubmissions);
        const auto delay = TDuration::MilliSeconds(Min<ui32>(1u << (op->RetryCount - 1), 100));
        const ui64 id = ++NextRetryId;
        DelayedRetries.emplace(id, TPendingIoOp(std::move(op)));
        Schedule(delay, new TEvPrivate::TEvRetryIODelayed(id));
    }

    void TDDiskActor::HandleRetryIODelayed(TEvPrivate::TEvRetryIODelayed::TPtr ev) {
        const auto it = DelayedRetries.find(ev->Get()->Id);
        if (it == DelayedRetries.end()) {
            return;
        }
        auto op = std::move(it->second.Op);
        DelayedRetries.erase(it);
        if (Stopping || IsBroken()) {
            CancelPendingIo(std::move(op));
            return;
        }
        DirectUringOp(op, /*isRetry=*/true);
    }

    void TDDiskActor::HandleWakeup(TEvents::TEvWakeup::TPtr &ev) {
        switch (ev->Get()->Tag) {
            case EWakeupTag::WakeupCollectMemoryMetrics: {
                CollectMemoryMetrics();
                break;
            }
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
        }
    }

} // NKikimr::NDDisk
