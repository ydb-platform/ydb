#include "chunk_keeper_actor.h"
#include "chunk_keeper_events.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/util/stlog.h>

#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

class TChunkKeeperActor : public TActorBootstrapped<TChunkKeeperActor> {
private:
    struct TRequestAllocate {
        TActorId Sender;
        ui32 Subsystem;
        std::optional<ui32> AllocatedChunk = {};
    };

    struct TRequestFree {
        TActorId Sender;
        ui32 ChunkIdx;
        ui32 Subsystem;
    };

    struct TRequestCutLog {};

    using TRequest = std::variant<TRequestAllocate, TRequestFree, TRequestCutLog>;

public:
    TChunkKeeperActor(TChunkKeeperCtx&& ctx, std::unique_ptr<TChunkKeeperData>&& data,
            bool isActive, bool readOnly)
        : Ctx(std::move(ctx))
        , Committed(std::move(data))
        , IsActive(isActive)
        , ReadOnly(readOnly)
    {}

    void Bootstrap() {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK07, VDISKP(Ctx.LogCtx->VCtx, "Bootstrap TChunkKeeperActor"),
                (CommittedData, PrintCommittedData()),
                (IsActive, IsActive),
                (ReadOnly, ReadOnly));
        if (IsActive) {
            Become(&TThis::StateAccepting);
        } else {
            if (!ReadOnly) {
                if (!Committed->Chunks.empty()) {
                    DropAllChunks();
                } else {
                    Send(Ctx.LogCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::ChunkKeeper, Max<ui64>()));
                }
            }
            Become(&TThis::StateError);
        }
    }

private:
    STRICT_STFUNC(StateAccepting,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvCutLog, Handle)
        hFunc(TEvListChunks, Handle)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(StateProcessing,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvChunkReserveResult, Handle)
        hFunc(NPDisk::TEvCutLog, Handle)
        hFunc(NPDisk::TEvLogResult, Handle)
        hFunc(TEvListChunks, Handle)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(TerminateStateFunc,
        IgnoreFunc(TEvChunkKeeperAllocate)
        IgnoreFunc(TEvChunkKeeperFree)
        IgnoreFunc(TEvChunkKeeperDiscover)
        IgnoreFunc(NPDisk::TEvCutLog)
        IgnoreFunc(TEvListChunks)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(StateError,
        hFunc(TEvChunkKeeperAllocate, HandleError)
        hFunc(TEvChunkKeeperFree, HandleError)
        hFunc(TEvChunkKeeperDiscover, HandleError)
        hFunc(NPDisk::TEvLogResult, HandleError)
        hFunc(TEvListChunks, HandleError)
        IgnoreFunc(NPDisk::TEvCutLog)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

private:

    void Handle(const NPDisk::TEvLogResult::TPtr& ev) {
        Y_VERIFY_S(ActiveRequest, VDISKP(Ctx.LogCtx->VCtx, "No ActiveRequest while handling TEvLogResult"));
        TRequest activeRequest = *ActiveRequest;
        std::visit(TOverloaded{
            [&](const TRequestAllocate&) -> void {
                HandleLogResultAllocate(ev);
            },
            [&](const TRequestFree&) -> void {
                HandleLogResultFree(ev);
            },
            [&](const TRequestCutLog&) -> void {
                HandleLogResultCutLog(ev);
            },
            [&](const std::monostate&) -> void { Y_ABORT_S(VDISKP(Ctx.LogCtx->VCtx,
                    "Empty ActiveRequest while processing request")); },
        }, activeRequest);
    }

    void Handle(const TEvChunkKeeperAllocate::TPtr& ev) {
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK02, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvChunkKeeperAllocate"),
                (Subsystem, subsystem));
        { // validation
            if (ReadOnly) {
                TString errorReason = TStringBuilder() << "ChunkKeeper is in read-only mode, Subsystem# " << subsystem;
                STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK04, VDISKP(Ctx.LogCtx->VCtx, "Bad allocation request"),
                        (ErrorReason, errorReason));
                Send(ev->Sender, new TEvChunkKeeperAllocateResult(std::nullopt, NKikimrProto::ERROR, errorReason));
                return;
            }
        }
        PendingRequests.push_back(TRequest(TRequestAllocate{
            .Sender = ev->Sender,
            .Subsystem = subsystem,
        }));
        ProcessRequestQueue();
    }
    void Handle(const TEvChunkKeeperFree::TPtr& ev) {
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK03, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvChunkKeeperFree"),
                (ChunkIdx, chunkIdx),
                (Subsystem, subsystem));

        { // validation
            if (ReadOnly) {
                TString errorReason = TStringBuilder() << "ChunkKeeper is in read-only mode, ChunkIdx# " << chunkIdx << " Subsystem# " << subsystem;
                STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK04, VDISKP(Ctx.LogCtx->VCtx, "Bad deallocation request"),
                        (ErrorReason, errorReason));
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, errorReason));
                return;
            }
            auto it = Committed->Chunks.find(ev->Get()->ChunkIdx);
            if (it == Committed->Chunks.end()) {
                TString errorReason = TStringBuilder() << "Chunk with idx# " << chunkIdx << " is not allocated";
                STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK04, VDISKP(Ctx.LogCtx->VCtx, "Bad deallocation request"),
                        (ErrorReason, errorReason));
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, errorReason));
                return;
            }
            if (it->second.Subsystem != subsystem) {
                TString errorReason = TStringBuilder() << "Chunk with idx# " << chunkIdx <<
                        " belongs to another subsystem Requested# " << subsystem << " Actual# " <<
                        it->second.Subsystem;
                STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK05, VDISKP(Ctx.LogCtx->VCtx, "Bad deallocation request"),
                        (ErrorReason, errorReason));
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, errorReason));
                return;
            }

        }
        PendingRequests.push_back(TRequest(TRequestFree{
            .Sender = ev->Sender,
            .ChunkIdx = chunkIdx,
            .Subsystem = subsystem,
        }));
        ProcessRequestQueue();
    }

    void Handle(const TEvChunkKeeperDiscover::TPtr& ev) {
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        auto it = Committed->ChunksBySubsystem.find(subsystem);
        ui32 discoveredChunks = 0;
        if (it == Committed->ChunksBySubsystem.end()) {
            Send(ev->Sender, new TEvChunkKeeperDiscoverResult({}, NKikimrProto::OK));
        } else {
            discoveredChunks = it->second.size();
            std::vector<TEvChunkKeeperDiscoverResult::TChunkInfo> res;
            std::transform(it->second.begin(), it->second.end(), std::back_inserter(res), [&](ui32 chunkIdx) {
                const auto it = Committed->Chunks.find(chunkIdx);
                Y_VERIFY_S(it != Committed->Chunks.end(), Ctx.LogCtx->VCtx->VDiskLogPrefix <<
                        "Chunk not found, ChunkIdx# " << chunkIdx << " Subsystem# " << subsystem);
                return TEvChunkKeeperDiscoverResult::TChunkInfo{
                    .ChunkIdx = chunkIdx,
                    .ShredRequested = it->second.ShredRequested,
                };
            });
            Send(ev->Sender, new TEvChunkKeeperDiscoverResult(std::move(res), NKikimrProto::OK));
        }
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK06, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvChunkKeeperDiscover"),
                (Subsystem, subsystem),
                (DiscoveredChunks, discoveredChunks));
    }

    void Handle(const NPDisk::TEvCutLog::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK18, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvCutLog"),
                (CurEntryPointLsn, CurEntryPointLsn),
                (FreeUpToLsn, ev->Get()->FreeUpToLsn));
        if (ReadOnly) {
            return;
        }
        if (CurEntryPointLsn < ev->Get()->FreeUpToLsn) {
            PendingRequests.push_front(TRequestCutLog{});
        }
        ProcessRequestQueue();
    }

    TString PrintCommittedData() {
        // print owned chunks
        TStringStream str;
        str << "[";
        for (const auto& [subsystem, chunks] : Committed->ChunksBySubsystem) {
            str << "{ Subsystem# " << subsystem;
            str << " Chunks# [ ";
            for (const ui32 chunkIdx : chunks) {
                str << chunkIdx << " ";
            }
            str << "] } ";
        }
        str << "]";
        return str.Str();
    }

    void ProcessRequestQueue() {
        if (ActiveRequest) {
            return; // wait for active request to finish
        }
        if (PendingRequests.empty()) {
            Become(&TThis::StateAccepting);
            return; // no requests pending
        }

        ActiveRequest = PendingRequests.front();
        PendingRequests.pop_front();

        std::visit(TOverloaded{
            [&](const TRequestAllocate& request) -> void { ProcessRequestAllocate(request); },
            [&](const TRequestFree& request) -> void { ProcessRequestFree(request); },
            [&](const TRequestCutLog& request) -> void { ProcessRequestCutLog(request); },
            [&](const std::monostate&) -> void { Y_ABORT_S(VDISKP(Ctx.LogCtx->VCtx,
                    "Empty ActiveRequest while processing allocation request")); },
        }, *ActiveRequest);
    }

    void ProcessRequestAllocate(TRequestAllocate request) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK09, VDISKP(Ctx.LogCtx->VCtx, "Process allocation request"),
                (Subsystem, request.Subsystem));
        Become(&TThis::StateProcessing);
        auto msg = std::make_unique<NPDisk::TEvChunkReserve>(Ctx.LogCtx->PDiskCtx->Dsk->Owner,
                Ctx.LogCtx->PDiskCtx->Dsk->OwnerRound, 1);
        Send(Ctx.LogCtx->PDiskCtx->PDiskId, msg.release());
    }

    void Handle(const NPDisk::TEvChunkReserveResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK10, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvChunkReserveResult"),
                (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(Ctx.LogCtx->VCtx, ev, TActivationContext::AsActorContext());

        Y_VERIFY_S(ActiveRequest, VDISKP(Ctx.LogCtx->VCtx, "No ActiveRequest"
                "while processing allocation request"));
        TRequestAllocate* request;
        std::visit(TOverloaded{
            [&](TRequestAllocate& req) -> void { request = &req; },
            [&](const TRequestFree&) -> void { Y_ABORT_S(VDISKP(Ctx.LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing allocation request")); },
            [&](const TRequestCutLog&) -> void { Y_ABORT_S(VDISKP(Ctx.LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing allocation request")); },
            [&](const std::monostate&) -> void { Y_ABORT_S(VDISKP(Ctx.LogCtx->VCtx,
                    "Empty ActiveRequest while processing allocation request")); },
        }, *ActiveRequest);

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->ChunkIds.size() == 1, Ctx.LogCtx->VCtx->VDiskLogPrefix <<
                    "Allocated wrong number of chunks# " << ev->Get()->ToString());
            ui32 chunkIdx = ev->Get()->ChunkIds[0];
            request->AllocatedChunk = chunkIdx;
            ui32 subsystem = request->Subsystem;

            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(chunkIdx);
            Y_VERIFY_S(AllocationsInFlight.count(chunkIdx) == 0, Ctx.LogCtx->VCtx->VDiskLogPrefix <<
                    "AllocationsInFlight is inconsistent, ChunkIdx# " << chunkIdx);
            AllocationsInFlight[chunkIdx] = subsystem;
            IssueCommit(std::move(commitRecord));
            return;
        }
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK21, VDISKP(Ctx.LogCtx->VCtx, "Unsuccessful ChunkReserve request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            Send(request->Sender, new TEvChunkKeeperAllocateResult({}, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            ActiveRequest.reset();
            ProcessRequestQueue();
            return;
        }
    }

    void HandleLogResultAllocate(const NPDisk::TEvLogResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK11, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvLogResult while processing"
                " allocation request"), (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(Ctx.LogCtx->VCtx, ev, TActivationContext::AsActorContext());

        Y_VERIFY_S(ActiveRequest, VDISKP(Ctx.LogCtx->VCtx, "No ActiveRequest while processing allocation request"));
        const TRequestAllocate request = std::get<TRequestAllocate>(*std::exchange(ActiveRequest, std::nullopt));
        Y_VERIFY_S(request.AllocatedChunk, VDISKP(Ctx.LogCtx->VCtx, "AllocatedChunk is expected to be non-nullopt"));
        const ui32 chunkIdx = *request.AllocatedChunk;
        const ui32 subsystem = request.Subsystem;
        const TActorId sender = request.Sender;

        {
            bool erased = AllocationsInFlight.erase(chunkIdx);
            Y_VERIFY_S(erased, Ctx.LogCtx->VCtx->VDiskLogPrefix << "AllocationsInFlight is inconsistent,"
                    " ChunkIdx# " << chunkIdx);
        }

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->Results.size() == 1, Ctx.LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());

            const auto [_, inserted] = Committed->ChunksBySubsystem[subsystem].insert(chunkIdx);
            Y_VERIFY_S(inserted, Ctx.LogCtx->VCtx->VDiskLogPrefix << "Bad chunk allocation, chunkIdx# " << chunkIdx <<
                    " subsystem# " << subsystem << " previous subsystem# " << Committed->Chunks[chunkIdx].Subsystem);
            Committed->Chunks[chunkIdx] = TChunkRecord{
                .ChunkIdx = chunkIdx,
                .Subsystem = subsystem,
                .ShredRequested = false,
            };

            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            SendCutLog();
            Send(sender, new TEvChunkKeeperAllocateResult(chunkIdx, NKikimrProto::OK));
            break;
        }
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK22, VDISKP(Ctx.LogCtx->VCtx, "Unsuccessful Log request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            Send(sender, new TEvChunkKeeperAllocateResult(chunkIdx, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void ProcessRequestFree(TRequestFree request) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK13, VDISKP(Ctx.LogCtx->VCtx, "Processing deallocation request"),
                (Subsystem, request.Subsystem),
                (ChunkIdx, request.ChunkIdx));
        auto it = Committed->Chunks.find(request.ChunkIdx);
        if (it == Committed->Chunks.end() || it->second.Subsystem != request.Subsystem) {
            TString errorReason = TStringBuilder() << "Chunk doesn't belong to this subsystem, possible double-free, "
                    "ChunkIdx# " << request.ChunkIdx;
            STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK12, VDISKP(Ctx.LogCtx->VCtx, "Race occurred"),
                    (ErrorReason, errorReason));
            Send(request.Sender, new TEvChunkKeeperFreeResult(request.ChunkIdx, NKikimrProto::ERROR, errorReason));
            ActiveRequest.reset();
            ProcessRequestQueue();
        } else {
            Become(&TThis::StateProcessing);
            NPDisk::TCommitRecord commitRecord;
            commitRecord.DeleteChunks.push_back(request.ChunkIdx);
            Y_VERIFY_S(DeletionsInFlight.count(request.ChunkIdx) == 0, Ctx.LogCtx->VCtx->VDiskLogPrefix <<
                    "DeletionsInFlight is inconsistent, ChunkIdx# " << request.ChunkIdx);
            DeletionsInFlight.insert(request.ChunkIdx);
            IssueCommit(std::move(commitRecord));
        }
    }

    void HandleLogResultFree(const NPDisk::TEvLogResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK14, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvLogResult while processing"
                " deallocation request"), (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(Ctx.LogCtx->VCtx, ev, TActivationContext::AsActorContext());

        Y_VERIFY_S(ActiveRequest, VDISKP(Ctx.LogCtx->VCtx, "No ActiveRequest while processing deallocation request"));
        const TRequestFree request = std::get<TRequestFree>(*std::exchange(ActiveRequest, std::nullopt));
        const ui32 chunkIdx = request.ChunkIdx;
        const ui32 subsystem = request.Subsystem;
        const TActorId sender = request.Sender;

        {
            bool erased = DeletionsInFlight.erase(chunkIdx);
            Y_VERIFY_S(erased, Ctx.LogCtx->VCtx->VDiskLogPrefix << "DeletionsInFlight is inconsistent,"
                    " ChunkIdx# " << chunkIdx);
        }

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->Results.size() == 1, Ctx.LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());

            {
                auto it = Committed->Chunks.find(chunkIdx);
                Y_VERIFY_S(it != Committed->Chunks.end(), Ctx.LogCtx->VCtx->VDiskLogPrefix << "Internal structure corrupted, chunkIdx# " <<
                        chunkIdx << " subsystem# " << subsystem);
                Committed->Chunks.erase(it);
            }
            {
                auto& chunks = Committed->ChunksBySubsystem[subsystem];
                auto it = chunks.find(chunkIdx);
                Y_VERIFY_S(it != chunks.end(), Ctx.LogCtx->VCtx->VDiskLogPrefix << "Internal structure corrupted, chunkIdx# " <<
                        chunkIdx << " subsystem# " << subsystem);
                chunks.erase(it);
            }

            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            SendCutLog();
            Send(sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::OK));
            Send(Ctx.SkeletonId, new TEvNotifyChunksDeleted(LastAllocatedLsn, {chunkIdx}));
            break;
        }
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK23, VDISKP(Ctx.LogCtx->VCtx, "Unsuccessful Log request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            Send(sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void ProcessRequestCutLog(TRequestCutLog) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK20, VDISKP(Ctx.LogCtx->VCtx, "Processing cut log request"));
        Become(&TThis::StateProcessing);
        IssueCommit(NPDisk::TCommitRecord{});
    }

    void HandleLogResultCutLog(const NPDisk::TEvLogResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK17, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvLogResult while processing cut log request"),
                (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(Ctx.LogCtx->VCtx, ev, TActivationContext::AsActorContext());
        ActiveRequest.reset();

        switch (ev->Get()->Status) {
        case NKikimrProto::OK:
            Y_VERIFY_S(ev->Get()->Results.size() == 1, Ctx.LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());
            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            SendCutLog();
            break;
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK24, VDISKP(Ctx.LogCtx->VCtx, "Unsuccessful Log request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void IssueCommit(NPDisk::TCommitRecord&& commitRecord) {
        commitRecord.IsStartingPoint = true;
        TLsnSeg seg = Ctx.LogCtx->LsnMngr->AllocLsnForLocalUse();
        LastAllocatedLsn = seg.Last;

        auto commitMsg = std::make_unique<NPDisk::TEvLog>(Ctx.LogCtx->PDiskCtx->Dsk->Owner,
                Ctx.LogCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureChunkKeeper,
                std::move(commitRecord), SerializeEntryPoint(), seg, nullptr);

        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK15, VDISKP(Ctx.LogCtx->VCtx, "Sending TEvLog"),
                (Event, commitMsg->ToString()));
        Send(Ctx.LogCtx->LoggerId, commitMsg.release());
    }

    void SendCutLog() {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK19, VDISKP(Ctx.LogCtx->VCtx, "Sending TEvVDiskCutLog"),
                (CurEntryPointLsn, CurEntryPointLsn));
        Send(Ctx.LogCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::ChunkKeeper, CurEntryPointLsn));
    }

    TRcBuf SerializeEntryPoint() {
        NKikimrVDiskData::TChunkKeeperEntryPoint proto;
        auto serializeChunk = [&](ui32 chunkIdx, ui32 subsystem, bool committed) {
            if (committed) {
                if (DeletionsInFlight.contains(chunkIdx)) {
                    // chunk is being deleted, generate commit without this chunk
                    return;
                }
            } else {
                Y_VERIFY_DEBUG_S(!DeletionsInFlight.contains(chunkIdx), Ctx.LogCtx->VCtx->VDiskLogPrefix << 
                        "Chunk is being allocated and deleted simultaneously, ChunkIdx# " << chunkIdx);
            }
            auto* chunk = proto.AddChunks();
            chunk->SetChunkIdx(chunkIdx);
            chunk->SetSubsystem(subsystem);
        };

        for (const auto& [chunkIdx, chunkRecord] : Committed->Chunks) {
            serializeChunk(chunkIdx, chunkRecord.Subsystem, true);
        }
        for (const auto& [chunkIdx, subsystem] : AllocationsInFlight) {
            serializeChunk(chunkIdx, subsystem, false);
        }

        TRcBuf data(TRcBuf::Uninitialized(proto.ByteSizeLong()));
        const bool success = proto.SerializeToArray(reinterpret_cast<uint8_t*>(
                data.UnsafeGetDataMut()), data.GetSize());
        Y_VERIFY_S(success, VDISKP(Ctx.LogCtx->VCtx, "Failed to serialize entry point"));

#ifndef NDEBUG
        // print text version of entrypoint
        TString str;
        google::protobuf::TextFormat::PrintToString(proto, &str);

        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK25, VDISKP(Ctx.LogCtx->VCtx, "Printing entrypoint"),
                (CommittedChunks, Committed->Chunks.size()),
                (AllocationsInFlight, AllocationsInFlight.size()),
                (DeletionsInFlight, DeletionsInFlight.size()),
                (ProtoSize, proto.ByteSizeLong()),
                (Text, str));
#endif
        return data;
    }

    void Handle(const TEvListChunks::TPtr& ev) {
        std::unique_ptr<TEvListChunksResult> res = std::make_unique<TEvListChunksResult>();

        auto processList = [&](auto& list) {
            for (auto& [chunkIdx, chunk] : list) {
                if (ev->Get()->ChunksOfInterest.contains(chunkIdx)) {
                    res->ChunksChunkKeeper.insert(chunkIdx);
                    if constexpr(std::is_same_v<std::decay_t<decltype(chunk)>, TChunkRecord>) {
                        chunk.ShredRequested = true;
                    }
                }
            }
        };
        processList(Committed->Chunks);
        processList(AllocationsInFlight);

        Send(ev->Sender, res.release());
    }

    // Error State

    void DropAllChunks() {
        NPDisk::TCommitRecord commitRecord;
        for (const auto& [chunkIdx, _] : Committed->Chunks) {
            DeletionsInFlight.insert(chunkIdx);
            commitRecord.DeleteChunks.push_back(chunkIdx);
        }
        IssueCommit(std::move(commitRecord));
    }

    void HandleError(const TEvChunkKeeperAllocate::TPtr& ev) {
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        STLOG(PRI_NOTICE, BS_CHUNK_KEEPER, BSCK02, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvChunkKeeperAllocate in ErrorState"),
                (Subsystem, subsystem));
        Send(ev->Sender, new TEvChunkKeeperAllocateResult(std::nullopt, NKikimrProto::ERROR, "ChunkKeeper is disabled"));
    }

    void HandleError(const TEvChunkKeeperFree::TPtr& ev) {
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        STLOG(PRI_NOTICE, BS_CHUNK_KEEPER, BSCK02, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvChunkKeeperFree in ErrorState"),
                (ChunkIdx, chunkIdx),
                (Subsystem, subsystem));
        Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, "ChunkKeeper is disabled"));
    }

    void HandleError(const TEvChunkKeeperDiscover::TPtr& ev) {
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        STLOG(PRI_NOTICE, BS_CHUNK_KEEPER, BSCK02, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvChunkKeeperDiscover in ErrorState"),
                (Subsystem, subsystem));
        Send(ev->Sender, new TEvChunkKeeperDiscoverResult({}, NKikimrProto::ERROR, "ChunkKeeper is disabled"));
    }

    void HandleError(const NPDisk::TEvLogResult::TPtr& ev) {
        STLOG(PRI_NOTICE, BS_CHUNK_KEEPER, BSCK02, VDISKP(Ctx.LogCtx->VCtx, "Handle TEvLogResult in ErrorState"),
                (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(Ctx.LogCtx->VCtx, ev, TActivationContext::AsActorContext());
        Send(Ctx.LogCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::ChunkKeeper, Max<ui64>()));
        DeletionsInFlight.clear();
        Committed.reset();  // ensure it won't be used
    }


    void HandleError(const TEvListChunks::TPtr& ev) {
        // If ChunkKeeper is disabled, all chunks are deallocated on start when VDisk is not ReadOnly
        // If VDisk is ReadOnly, shredding doesn't work at all
        Send(ev->Sender, new TEvListChunksResult);
    }

private:
    const TChunkKeeperCtx Ctx;

    // in-flight unfinished commits
    std::unordered_map<ui32, ui32> AllocationsInFlight;
    std::unordered_set<ui32> DeletionsInFlight;

    std::unique_ptr<TChunkKeeperData> Committed;

    std::deque<TRequest> PendingRequests;
    std::optional<TRequest> ActiveRequest;

    ui64 CurEntryPointLsn = 0;
    ui64 LastAllocatedLsn = 0;
    bool IsActive;
    bool ReadOnly;
};

IActor* CreateChunkKeeperActor(TChunkKeeperCtx&& ctx, std::unique_ptr<TChunkKeeperData>&& data,
        bool isActive, bool readOnly) {
    return new TChunkKeeperActor(std::move(ctx), std::move(data), isActive, readOnly);
}

} // namespace NKikimr
