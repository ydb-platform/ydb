#include "chunk_keeper_actor.h"
#include "chunk_keeper_events.h"
#include <ydb/core/protos/blobstorage_vdisk_internal.proto>

namespace NKikimr {

class TChunkKeeperActor : public TActorBootstrapped<TChunkKeeperActor> {
public:
    TChunkKeeperActor(TIntrusivePtr<TVDiskContext> vctx,
            NKikimrVDiskData::TChunkKeeperEntryPoint entryPoint)
        : VCtx(vctx)
        , EntryPoint(entryPoint)
    {
        ParseEntryPoint(entryPoint);
    }

    void Bootstrap() {

    }

private:
    STRICT_STFUNC(StateAcceptingReqeuests,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
    )

    STRICT_STFUNC(StateHandling,
        hFunc(TEvChunkKeeperAllocate, Handle)
        hFunc(TEvChunkKeeperFree, Handle)
    )

    void Handle(const TEvChunkKeeperAllocate::TPtr& ev) {
        PendingRequests.push_back(TRequest(TRequestAllocate{
            .Sender = ev->Sender,
            .ChunkOwner = ev->Get()->ChunkOwner,
        }));
        ProcessRequestQueue();
    }

    void Handle(const TEvChunkKeeperFree::TPtr& ev) {
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        ui32 chunkOwner = ev->Get()->ChunkOwner;

        { // validation
            auto it = OwnedChunks.find(ev->Get()->ChunkIdx);
            if (it == OwnedChunks.end()) {
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR,
                        TStringBuilder() << "Chunk with idx# " << ChunkIdx << "is not allocated"));
                return;
            }
            if (it->second != chunkOwner) {
                Send(ev->Sender, new TEvChunkKeeperFreeResult(chunkIdx, NKikimrProto::ERROR,
                        TStringBuilder() << "Chunk with idx# " << ChunkIdx << "belongs to another owner"
                                            " Requested# " << chunkOwner << " Actual# " << it->second));
                return;
            }

        }
        PendingRequests.push_back(TRequest(TRequestFree{
            .Sender = ev->Sender,
            .ChunkIdx = ev->Get()->ChunkIdx,
            .ChunkOwner = ev->Get()->ChunkOwner,
        }));
        ProcessRequestQueue();
    }

    void ParseEntryPoint(const NKikimrVDiskData::TChunkKeeperEntryPoint& entryPoint) {
        for (const NKikimrVDiskData::TChunkKeeperEntryPoint::TOwnedChunk& chunk : entryPoint.GetOwnedChunks()) {
            ui32 chunkIdx = chunk.GetChunkIdx();
            ui32 chunkOwner = chunk.GetChunkOwner();
            {
                auto it = OwnedChunks.find();
                Y_ABORT_UNLESS(it == OwnedChunks.end(), "Double ownership detected, ChunkIdx# " << chunkIdx <<
                        " ChunkOwner# " << chunkOwner << " another ChunkOwner# " << it->second);
            }

            OwnedChunks[chunkIdx] = chunkOwner;
        }

        for (const ui32 chunkIdx : entryPoint.GetChunksToDelete()) {
            ChunksToDelete.push_back(chunkIdx);
        }
    }

    void ProcessRequestQueue() {
        if (ActiveRequest) {
            return; // wait for active request to finish
        }
        if (RequestQueue.empty()) {
            return; // no requests pending
        }

        ActiveRequest = RequestQueue.front();
        RequestQueue.pop_front();

        std::visit(TOverloaded{
            [](const TRequestAllocate& request) { },
        }, ActiveRequest);
    }

    void ProcessRequestAllocate(TRequestAllocate request) {
        
    }

    void ProcessRequestFree(TRequestAllocate request) {
        
    }

private:
    class TRequestAllocate {
        TActorId Sender;
        ui32 ChunkOwner;
    };

    class TRequestFree {
        TActorId Sender;
        ui32 ChunkIdx;
        ui32 ChunkOwner;
    };

    using TRequest = std::variant<TRequestAllocate, TRequestFree>;

private:
    TIntrusivePtr<TVDiskContext> VCtx;
    NKikimrVDiskData::TChunkKeeperEntryPoint EntryPoint;

    std::unordered_map<ui32, ui32> OwnedChunks; // ChunkId -> Owner
    std::vector<ui32> ChunksToDelete;

    std::deque<TRequest> PendingRequests;
    std::optional<TRequest> ActiveRequest;
};

IActor* CreateChunkKeeperActor(TIntrusivePtr<TVDiskContext> vctx,
        NKikimrVDiskData::TChunkKeeperEntryPoint entryPoint) {

}

} // namespace NKikimr
