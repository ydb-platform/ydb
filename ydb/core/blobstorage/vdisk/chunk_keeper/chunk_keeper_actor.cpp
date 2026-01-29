#include "chunk_keeper_actor.h"
#include "chunk_keeper_events.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
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
    TChunkKeeperActor(const TIntrusivePtr<TVDiskLogContext>& logCtx,
            std::unique_ptr<TChunkKeeperData>&& data)
        : LogCtx(logCtx)
        , Committed(std::move(data))
    {}

    void Bootstrap() {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK07, VDISKP(LogCtx->VCtx, "Bootstrap TChunkKeeperActor"),
                (CommittedData, PrintCommittedData()));
        Become(&TThis::StateAccepting);
    }

private:
    STRICT_STFUNC(StateAccepting,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvCutLog, Handle)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(StateProcessingAllocate,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvChunkReserveResult, Handle)
        hFunc(NPDisk::TEvCutLog, Handle)
        hFunc(NPDisk::TEvLogResult, HandleLogResultAllocate)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(StateProcessingFree,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvCutLog, Handle)
        hFunc(NPDisk::TEvLogResult, HandleLogResultFree)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(StateProcessingCutLog,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvCutLog, Handle)
        hFunc(NPDisk::TEvLogResult, HandleLogResultCutLog)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(TerminateStateFunc,
        IgnoreFunc(TEvChunkKeeperAllocate)
        IgnoreFunc(TEvChunkKeeperFree)
        IgnoreFunc(TEvChunkKeeperDiscover)
        IgnoreFunc(NPDisk::TEvCutLog)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

private:

    void Handle(const TEvChunkKeeperAllocate::TPtr& ev) {
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK02, VDISKP(LogCtx->VCtx, "Handle TEvChunkKeeperAllocate"),
                (Subsystem, subsystem));
        PendingRequests.push_back(TRequest(TRequestAllocate{
            .Sender = ev->Sender,
            .Subsystem = subsystem,
        }));
        ProcessRequestQueue();
    }

    void Handle(const TEvChunkKeeperFree::TPtr& ev) {
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK03, VDISKP(LogCtx->VCtx, "Handle TEvChunkKeeperFree"),
                (ChunkIdx, chunkIdx),
                (Subsystem, subsystem));

        { // validation
            auto it = Committed->Chunks.find(ev->Get()->ChunkIdx);
            if (it == Committed->Chunks.end()) {
                TString errorReason = TStringBuilder() << "Chunk with idx# " << chunkIdx << " is not allocated";
                STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK04, VDISKP(LogCtx->VCtx, "Bad deallocation request"),
                        (ErrorReason, errorReason));
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, errorReason));
                return;
            }
            if (it->second != subsystem) {
                TString errorReason = TStringBuilder() << "Chunk with idx# " << chunkIdx <<
                        " belongs to another subsystem Requested# " << subsystem << " Actual# " << it->second;
                STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK05, VDISKP(LogCtx->VCtx, "Bad deallocation request"),
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
            Send(ev->Sender, new TEvChunkKeeperDiscoverResult({}));
        } else {
            discoveredChunks = it->second.size();
            Send(ev->Sender, new TEvChunkKeeperDiscoverResult(
                    std::vector<ui32>(it->second.begin(), it->second.end())));
        }
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK06, VDISKP(LogCtx->VCtx, "Handle TEvChunkKeeperDiscover"),
                (Subsystem, subsystem),
                (DiscoveredChunks, discoveredChunks));
    }

    void Handle(const NPDisk::TEvCutLog::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK18, VDISKP(LogCtx->VCtx, "Handle TEvCutLog"),
                (CurEntryPointLsn, CurEntryPointLsn),
                (FreeUpToLsn, ev->Get()->FreeUpToLsn));
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
            [&](const std::monostate&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Empty ActiveRequest while processing allocation request")); },
        }, *ActiveRequest);
    }

    void ProcessRequestAllocate(TRequestAllocate request) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK09, VDISKP(LogCtx->VCtx, "Process allocation request"),
                (Subsystem, request.Subsystem));
        Become(&TThis::StateProcessingAllocate);
        auto msg = std::make_unique<NPDisk::TEvChunkReserve>(LogCtx->PDiskCtx->Dsk->Owner,
                LogCtx->PDiskCtx->Dsk->OwnerRound, 1);
        Send(LogCtx->PDiskCtx->PDiskId, msg.release());
    }

    void Handle(const NPDisk::TEvChunkReserveResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK10, VDISKP(LogCtx->VCtx, "Handle TEvChunkReserveResult"),
                (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());

        Y_VERIFY_S(ActiveRequest, VDISKP(LogCtx->VCtx, "No ActiveRequest"
                "while processing allocation request"));
        TRequestAllocate* request;
        std::visit(TOverloaded{
            [&](TRequestAllocate& req) -> void { request = &req; },
            [&](const TRequestFree&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing allocation request")); },
            [&](const TRequestCutLog&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing allocation request")); },
            [&](const std::monostate&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Empty ActiveRequest while processing allocation request")); },
        }, *ActiveRequest);

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->ChunkIds.size() == 1, LogCtx->VCtx->VDiskLogPrefix <<
                    "Allocated wrong number of chunks# " << ev->Get()->ToString());
            ui32 chunkIdx = ev->Get()->ChunkIds[0];
            request->AllocatedChunk = chunkIdx;
            ui32 subsystem = request->Subsystem;

            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(chunkIdx);
            Y_VERIFY_S(AllocationsInFlight.count(chunkIdx) == 0, LogCtx->VCtx->VDiskLogPrefix <<
                    "AllocationsInFlight is inconsistent, ChunkIdx# " << chunkIdx);
            AllocationsInFlight[chunkIdx] = subsystem;
            IssueCommit(std::move(commitRecord));
            return;
        }
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK21, VDISKP(LogCtx->VCtx, "Unsuccessful ChunkReserve request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            Send(request->Sender, new TEvChunkKeeperAllocateResult({}, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            ActiveRequest.reset();
            ProcessRequestQueue();
            return;
        }
    }

    void HandleLogResultAllocate(const NPDisk::TEvLogResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK11, VDISKP(LogCtx->VCtx, "Handle TEvLogResult while processing"
                " allocation request"), (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());

        ui32 chunkIdx;
        ui32 subsystem;
        TActorId sender;

        Y_VERIFY_S(ActiveRequest, VDISKP(LogCtx->VCtx, "No ActiveRequest while processing allocation request"));
        std::visit(TOverloaded{
            [&](const TRequestAllocate& request) -> void {
                Y_VERIFY_S(request.AllocatedChunk, VDISKP(LogCtx->VCtx,
                        "AllocatedChunk is expected to be non-nullopt"));
                chunkIdx = *request.AllocatedChunk;
                subsystem = request.Subsystem;
                sender = request.Sender;
            },
            [&](const TRequestFree&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing allocation request")); },
            [&](const TRequestCutLog&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing allocation request")); },
            [&](const std::monostate&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Empty ActiveRequest while processing allocation request")); },
        }, *std::exchange(ActiveRequest, std::nullopt));

        {
            bool erased = AllocationsInFlight.erase(chunkIdx);
            Y_VERIFY_S(erased, LogCtx->VCtx->VDiskLogPrefix << "AllocationsInFlight is inconsistent,"
                    " ChunkIdx# " << chunkIdx);
        }

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->Results.size() == 1, LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());

            const auto [_, inserted] = Committed->ChunksBySubsystem[subsystem].insert(chunkIdx);
            Y_VERIFY_S(inserted, LogCtx->VCtx->VDiskLogPrefix << "Bad chunk allocation, chunkIdx# " << chunkIdx <<
                    " subsystem# " << subsystem << " previous subsystem# " << Committed->Chunks[chunkIdx]);
            Committed->Chunks[chunkIdx] = subsystem;

            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            SendCutLog();
            Send(sender, new TEvChunkKeeperAllocateResult(chunkIdx, NKikimrProto::OK));
            break;
        }
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK22, VDISKP(LogCtx->VCtx, "Unsuccessful Log request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            Send(sender, new TEvChunkKeeperAllocateResult(chunkIdx, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void ProcessRequestFree(TRequestFree request) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK13, VDISKP(LogCtx->VCtx, "Processing deallocation request"),
                (Subsystem, request.Subsystem),
                (ChunkIdx, request.ChunkIdx));
        auto it = Committed->Chunks.find(request.ChunkIdx);
        if (it == Committed->Chunks.end() || it->second != request.Subsystem) {
            TString errorReason = TStringBuilder() << "Chunk doesn't belong to this subsystem, possible double-free, "
                    "ChunkIdx# " << request.ChunkIdx;
            STLOG(PRI_ERROR, BS_CHUNK_KEEPER, BSCK12, VDISKP(LogCtx->VCtx, "Race occurred"),
                    (ErrorReason, errorReason));
            Send(request.Sender, new TEvChunkKeeperFreeResult(request.ChunkIdx, NKikimrProto::ERROR, errorReason));
            ActiveRequest.reset();
            ProcessRequestQueue();
        } else {
            Become(&TThis::StateProcessingFree);
            NPDisk::TCommitRecord commitRecord;
            commitRecord.DeleteChunks.push_back(request.ChunkIdx);
            Y_VERIFY_S(DeletionsInFlight.count(request.ChunkIdx) == 0, LogCtx->VCtx->VDiskLogPrefix <<
                    "DeletionsInFlight is inconsistent, ChunkIdx# " << request.ChunkIdx);
            DeletionsInFlight.insert(request.ChunkIdx);
            IssueCommit(std::move(commitRecord));
        }
    }

    void HandleLogResultFree(const NPDisk::TEvLogResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK14, VDISKP(LogCtx->VCtx, "Handle TEvLogResult while processing"
                " deallocation request"), (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());
        ui32 chunkIdx;
        ui32 subsystem;
        TActorId sender;

        Y_VERIFY_S(ActiveRequest, VDISKP(LogCtx->VCtx, "No ActiveRequest while processing deallocation request"));
        std::visit(TOverloaded{
            [&](const TRequestAllocate&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing deallocation request")); },
            [&](const TRequestFree& request) -> void {
                chunkIdx = request.ChunkIdx;
                subsystem = request.Subsystem;
                sender = request.Sender;
            },
            [&](const TRequestCutLog&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Unexpected ActiveRequest while processing deallocation request")); },
            [&](const std::monostate&) -> void { Y_ABORT_S(VDISKP(LogCtx->VCtx,
                    "Empty ActiveRequest while processing deallocation request")); },
        }, *std::exchange(ActiveRequest, std::nullopt));

        {
            bool erased = DeletionsInFlight.erase(chunkIdx);
            Y_VERIFY_S(erased, LogCtx->VCtx->VDiskLogPrefix << "DeletionsInFlight is inconsistent,"
                    " ChunkIdx# " << chunkIdx);
        }

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->Results.size() == 1, LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());

            {
                auto it = Committed->Chunks.find(chunkIdx);
                Y_VERIFY_S(it != Committed->Chunks.end(), LogCtx->VCtx->VDiskLogPrefix << "Internal structure corrupted, chunkIdx# " <<
                        chunkIdx << " subsystem# " << subsystem);
                Committed->Chunks.erase(it);
            }
            {
                auto& chunks = Committed->ChunksBySubsystem[subsystem];
                auto it = chunks.find(chunkIdx);
                Y_VERIFY_S(it != chunks.end(), LogCtx->VCtx->VDiskLogPrefix << "Internal structure corrupted, chunkIdx# " <<
                        chunkIdx << " subsystem# " << subsystem);
                chunks.erase(it);
            }

            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            SendCutLog();
            Send(sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::OK));
            break;
        }
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK23, VDISKP(LogCtx->VCtx, "Unsuccessful Log request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            Send(sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void ProcessRequestCutLog(TRequestCutLog) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK20, VDISKP(LogCtx->VCtx, "Processing cut log request"));
        Become(&TThis::StateProcessingCutLog);
        IssueCommit(NPDisk::TCommitRecord{});
    }

    void HandleLogResultCutLog(const NPDisk::TEvLogResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK17, VDISKP(LogCtx->VCtx, "Handle TEvLogResult while processing cut log request"),
                (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());
        ActiveRequest.reset();

        switch (ev->Get()->Status) {
        case NKikimrProto::OK:
            Y_VERIFY_S(ev->Get()->Results.size() == 1, LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());
            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            SendCutLog();
            break;
        default:
            STLOG(PRI_WARN, BS_CHUNK_KEEPER, BSCK24, VDISKP(LogCtx->VCtx, "Unsuccessful Log request"),
                    (ErrorReason, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void IssueCommit(NPDisk::TCommitRecord&& commitRecord) {
        commitRecord.IsStartingPoint = true;
        TLsnSeg seg = LogCtx->LsnMngr->AllocLsnForLocalUse();

        auto commitMsg = std::make_unique<NPDisk::TEvLog>(LogCtx->PDiskCtx->Dsk->Owner,
                LogCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureChunkKeeper,
                std::move(commitRecord), SerializeEntryPoint(), seg, nullptr);

        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK15, VDISKP(LogCtx->VCtx, "Sending TEvLog"),
                (Event, commitMsg->ToString()));
        Send(LogCtx->LoggerId, commitMsg.release());
    }

    void SendCutLog() {
        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK19, VDISKP(LogCtx->VCtx, "Sending TEvVDiskCutLog"),
                (CurEntryPointLsn, CurEntryPointLsn));
        Send(LogCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::ChunkKeeper, CurEntryPointLsn));
    }

    TRcBuf SerializeEntryPoint() {
        NKikimrVDiskData::TChunkKeeperEntryPoint proto;
        auto serializeChunks = [&](const auto& chunks, bool committed) -> void {
            for (const auto& [chunkIdx, subsystem] : chunks) {
                if (committed) {
                    if (DeletionsInFlight.contains(chunkIdx)) {
                        // chunk is being deleted, generate commit without this chunk
                        continue;
                    }
                } else {
                    Y_VERIFY_DEBUG_S(!DeletionsInFlight.contains(chunkIdx), LogCtx->VCtx->VDiskLogPrefix << 
                            "Chunk is being allocated and deleted simultaneously, ChunkIdx# " << chunkIdx);
                }
                auto* chunk = proto.AddChunks();
                chunk->SetChunkIdx(chunkIdx);
                chunk->SetSubsystem(subsystem);
            }
        };
        serializeChunks(Committed->Chunks, true);
        serializeChunks(AllocationsInFlight, false);

        TRcBuf data(TRcBuf::Uninitialized(proto.ByteSizeLong()));
        const bool success = proto.SerializeToArray(reinterpret_cast<uint8_t*>(
                data.UnsafeGetDataMut()), data.GetSize());
        Y_VERIFY_S(success, VDISKP(LogCtx->VCtx, "Failed to serialize entry point"));

#ifndef NDEBUG
        // print text version of entrypoint
        TString str;
        google::protobuf::TextFormat::PrintToString(proto, &str);

        STLOG(PRI_DEBUG, BS_CHUNK_KEEPER, BSCK25, VDISKP(LogCtx->VCtx, "Printing entrypoint"),
                (CommittedChunks, Committed->Chunks.size()),
                (AllocationsInFlight, AllocationsInFlight.size()),
                (DeletionsInFlight, DeletionsInFlight.size()),
                (ProtoSize, proto.ByteSizeLong()),
                (Text, str));
#endif
        return data;
    }

private:
    const TIntrusivePtr<TVDiskLogContext> LogCtx;

    // in-flight unfinished commits
    std::unordered_map<ui32, ui32> AllocationsInFlight;
    std::unordered_set<ui32> DeletionsInFlight;

    std::unique_ptr<TChunkKeeperData> Committed;

    std::deque<TRequest> PendingRequests;
    std::optional<TRequest> ActiveRequest;

    ui64 CurEntryPointLsn = 0;
};

IActor* CreateChunkKeeperActor(const TIntrusivePtr<TVDiskLogContext>& logCtx,
        std::unique_ptr<TChunkKeeperData>&& data) {
    return new TChunkKeeperActor(logCtx, std::move(data));
}

} // namespace NKikimr
