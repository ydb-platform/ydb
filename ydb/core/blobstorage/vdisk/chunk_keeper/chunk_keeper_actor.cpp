#include "chunk_keeper_actor.h"
#include "chunk_keeper_events.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>

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

    using TRequest = std::variant<TRequestAllocate, TRequestFree>;

public:
    TChunkKeeperActor(TIntrusivePtr<TLogContext> logCtx,
            NKikimrVDiskData::TChunkKeeperEntryPoint entryPoint)
        : LogCtx(logCtx)
    {
        ParseEntryPoint(entryPoint);
    }

    void Bootstrap() {
        Become(&TThis::StateAccepting);
    }

private:
    STRICT_STFUNC(StateAccepting,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(StateProcessingAllocate,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvChunkReserveResult, Handle)
        hFunc(NPDisk::TEvLogResult, HandleLogResultAllocate)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(StateProcessingFree,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
        hFunc(TEvChunkKeeperDiscover, Handle)
        hFunc(NPDisk::TEvLogResult, HandleLogResultFree)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

    STRICT_STFUNC(TerminateStateFunc,
        IgnoreFunc(TEvChunkKeeperAllocate)
        IgnoreFunc(TEvChunkKeeperFree)
        IgnoreFunc(TEvChunkKeeperDiscover)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    )

private:

    void Handle(const TEvChunkKeeperAllocate::TPtr& ev) {
        PendingRequests.push_back(TRequest(TRequestAllocate{
            .Sender = ev->Sender,
            .Subsystem = static_cast<ui32>(ev->Get()->Subsystem),
        }));
        ProcessRequestQueue();
    }

    void Handle(const TEvChunkKeeperFree::TPtr& ev) {
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        ui32 subsystem = static_cast<ui32>(ev->Get()->Subsystem);

        { // validation
            auto it = Chunks.find(ev->Get()->ChunkIdx);
            if (it == Chunks.end()) {
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR,
                        TStringBuilder() << "Chunk with idx# " << chunkIdx << "is not allocated"));
                return;
            }
            if (it->second != subsystem) {
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR,
                        TStringBuilder() << "Chunk with idx# " << chunkIdx << "belongs to another subsystem"
                                            " Requested# " << subsystem << " Actual# " << it->second));
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
        auto it = ChunksBySubsystem.find(ev->Get()->Subsystem);
        if (it == ChunksBySubsystem.end()) {
            Send(ev->Sender, new TEvChunkKeeperDiscoverResult({}));
        } else {
            Send(ev->Sender, new TEvChunkKeeperDiscoverResult(
                    std::vector<ui32>(it->second.begin(), it->second.end())));
        }
    }

    void ParseEntryPoint(const NKikimrVDiskData::TChunkKeeperEntryPoint& entryPoint) {
        for (const NKikimrVDiskData::TChunkKeeperEntryPoint::TChunk& chunk : entryPoint.GetChunks()) {
            ui32 chunkIdx = chunk.GetChunkIdx();
            ui32 subsystem = chunk.GetSubsystem();
            Y_VERIFY_DEBUG_S(Chunks.find(chunkIdx) == Chunks.end(), LogCtx->VCtx->VDiskLogPrefix <<
                    "Double ownership detected, ChunkIdx# " << chunkIdx <<
                    " Subsystem# " << subsystem << " another Subsystem# " << Chunks[chunkIdx]);

            Chunks[chunkIdx] = subsystem;
            ChunksBySubsystem[subsystem].insert(chunkIdx);
        }
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
            [&](const std::monostate&) -> void { Y_ABORT_S(LogCtx->VCtx->VDiskLogPrefix <<
                    "Empty ActiveRequest while processing allocation request"); },
        }, *ActiveRequest);
    }

    void ProcessRequestAllocate(TRequestAllocate) {
        Become(&TThis::StateProcessingAllocate);
        auto msg = std::make_unique<NPDisk::TEvChunkReserve>(LogCtx->PDiskCtx->Dsk->Owner,
                LogCtx->PDiskCtx->Dsk->OwnerRound, 1);
        Send(LogCtx->PDiskCtx->PDiskId, msg.release());
    }

    void Handle(const NPDisk::TEvChunkReserveResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->ChunkIds.size() == 1, LogCtx->VCtx->VDiskLogPrefix <<
                    "Allocated wrong number of chunks# " << ev->Get()->ToString());
            ui32 chunkIdx = ev->Get()->ChunkIds[0];
            ui32 subsystem;

            Y_VERIFY_S(ActiveRequest, LogCtx->VCtx->VDiskLogPrefix << "No ActiveRequest"
                    "while processing allocation request");
            std::visit(TOverloaded{
                [&](TRequestAllocate& request) -> void {
                    request.AllocatedChunk = chunkIdx;
                    subsystem = request.Subsystem;
                },
                [&](TRequestFree&) -> void { Y_ABORT_S(LogCtx->VCtx->VDiskLogPrefix <<
                        "Unexpected ActiveRequest while processing allocation request"); },
                [&](std::monostate&) -> void { Y_ABORT_S(LogCtx->VCtx->VDiskLogPrefix <<
                        "Empty ActiveRequest while processing allocation request"); },
            }, *ActiveRequest);

            NPDisk::TCommitRecord commitRecord;
            commitRecord.CommitChunks.push_back(chunkIdx);
            IssueCommit(commitRecord);
            return;
        }
        default:
            Send(ev->Sender, new TEvChunkKeeperAllocateResult({}, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            ActiveRequest.reset();
            ProcessRequestQueue();
            return;
        }
    }

    void HandleLogResultAllocate(const NPDisk::TEvLogResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());
        ui32 chunkIdx;
        ui32 subsystem;
        TActorId sender;

        Y_VERIFY_S(ActiveRequest, LogCtx->VCtx->VDiskLogPrefix <<
                "No ActiveRequest while processing allocation request");
        std::visit(TOverloaded{
            [&](const TRequestAllocate& request) -> void {
                Y_VERIFY_S(request.AllocatedChunk, LogCtx->VCtx->VDiskLogPrefix <<
                        "AllocatedChunk is expected to be non-nullopt");
                chunkIdx = *request.AllocatedChunk;
                subsystem = request.Subsystem;
                sender = request.Sender;
            },
            [&](const TRequestFree&) -> void { Y_ABORT_S(LogCtx->VCtx->VDiskLogPrefix <<
                    "Unexpected ActiveRequest while processing allocation request"); },
            [&](const std::monostate&) -> void { Y_ABORT_S(LogCtx->VCtx->VDiskLogPrefix <<
                    "Empty ActiveRequest while processing allocation request"); },
        }, *std::exchange(ActiveRequest, std::nullopt));

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->Results.size() == 1, LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());

            const auto [_, inserted] = ChunksBySubsystem[subsystem].insert(chunkIdx);
            Y_VERIFY_S(inserted, LogCtx->VCtx->VDiskLogPrefix << "Bad chunk allocation, chunkIdx# " << chunkIdx <<
                    " subsystem# " << subsystem << " previous subsystem# " << Chunks[chunkIdx]);
            Chunks[chunkIdx] = subsystem;

            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            Send(LogCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::ChunkKeeper, CurEntryPointLsn));
            Send(sender, new TEvChunkKeeperAllocateResult(chunkIdx, NKikimrProto::OK));
            break;
        }
        default:
            Send(sender, new TEvChunkKeeperAllocateResult(chunkIdx, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void ProcessRequestFree(TRequestFree request) {
        auto it = Chunks.find(request.ChunkIdx);
        if (it == Chunks.end() || it->second != request.Subsystem) {
            Send(request.Sender, new TEvChunkKeeperFreeResult(request.ChunkIdx, NKikimrProto::ERROR, TStringBuilder() <<
                    "Chunk doesn't belong to this subsystem, possible double-free, ChunkIdx# " << request.ChunkIdx));
            ActiveRequest.reset();
            ProcessRequestQueue();
        } else {
            Become(&TThis::StateProcessingFree);
            NPDisk::TCommitRecord commitRecord;
            commitRecord.DeleteChunks.push_back(request.ChunkIdx);
            IssueCommit(commitRecord);
        }
    }

    void HandleLogResultFree(const NPDisk::TEvLogResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());
        ui32 chunkIdx;
        ui32 subsystem;
        TActorId sender;

        Y_VERIFY_S(ActiveRequest, LogCtx->VCtx->VDiskLogPrefix <<
                "No ActiveRequest while processing free request");
        std::visit(TOverloaded{
            [&](const TRequestAllocate&) -> void { Y_ABORT_S(LogCtx->VCtx->VDiskLogPrefix <<
                    "Unexpected ActiveRequest while processing free request"); },
            [&](const TRequestFree& request) -> void {
                chunkIdx = request.ChunkIdx;
                subsystem = request.Subsystem;
                sender = request.Sender;
            },
            [&](const std::monostate&) -> void { Y_ABORT_S(LogCtx->VCtx->VDiskLogPrefix <<
                    "Empty ActiveRequest while processing allocation request"); },
        }, *std::exchange(ActiveRequest, std::nullopt));

        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Y_VERIFY_S(ev->Get()->Results.size() == 1, LogCtx->VCtx->VDiskLogPrefix << "Bad TEvLogResult response# "
                    << ev->Get()->ToString());

            {
                auto it = Chunks.find(chunkIdx);
                Y_VERIFY_S(it != Chunks.end(), LogCtx->VCtx->VDiskLogPrefix << "Internal structure corrupted, chunkIdx# " <<
                        chunkIdx << " subsystem# " << subsystem);
                Chunks.erase(it);
            }
            {
                auto& chunks = ChunksBySubsystem[subsystem];
                auto it = chunks.find(chunkIdx);
                Y_VERIFY_S(it != chunks.end(), LogCtx->VCtx->VDiskLogPrefix << "Internal structure corrupted, chunkIdx# " <<
                        chunkIdx << " subsystem# " << subsystem);
                chunks.erase(it);
            }

            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            Send(LogCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::ChunkKeeper, CurEntryPointLsn));
            Send(sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::OK));
            break;
        }
        default:
            Send(sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR, ev->Get()->ErrorReason));
            break;
        }
        ProcessRequestQueue();
    }

    void IssueCommit(const NPDisk::TCommitRecord& commitRecord) {
        TLsnSeg seg = LogCtx->LsnMngr->AllocLsnForLocalUse();

        auto commitMsg = std::make_unique<NPDisk::TEvLog>(LogCtx->PDiskCtx->Dsk->Owner,
                LogCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureChunkKeeper,
                commitRecord, SerializeEntryPoint(), seg, nullptr);

        Send(LogCtx->LoggerId, commitMsg.release());
    }

    TRcBuf SerializeEntryPoint() {
        NKikimrVDiskData::TChunkKeeperEntryPoint proto;

        for (const auto& [chunkIdx, subsystem] : Chunks) {
            auto* chunk = proto.AddChunks();
            chunk->SetChunkIdx(chunkIdx);
            chunk->SetSubsystem(subsystem);
        }

        TRcBuf data(TRcBuf::Uninitialized(proto.ByteSizeLong()));
        const bool success = proto.SerializeToArray(reinterpret_cast<uint8_t*>(
                data.UnsafeGetDataMut()), data.GetSize());
        Y_VERIFY_S(success, LogCtx->VCtx->VDiskLogPrefix << "Failed to serialize entry point");
        return data;
    }

private:
    const TIntrusivePtr<TLogContext> LogCtx;
    const TPDiskCtxPtr PDiskCtx;

    std::unordered_map<ui32, ui32> Chunks;                      // ChunkId -> Subsystem
    std::unordered_map<ui32, std::set<ui32>> ChunksBySubsystem; // Subsystem -> [ Chunk1 .. ChunkN ]

    std::deque<TRequest> PendingRequests;
    std::optional<TRequest> ActiveRequest;

    ui64 CurEntryPointLsn = 0;
};

IActor* CreateChunkKeeperActor(TIntrusivePtr<TLogContext> logCtx,
        NKikimrVDiskData::TChunkKeeperEntryPoint entryPoint) {
    return new TChunkKeeperActor(logCtx, entryPoint);
}

} // namespace NKikimr
