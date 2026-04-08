#include "phantom_flags.h"
#include "phantom_flag_thresholds.h"
#include "phantom_flag_storage_processor.h"

#include <util/generic/overloaded.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/chunk_keeper/chunk_keeper_events.h>

#include <unordered_set>

namespace NKikimr::NSyncLog {

////////////////////////////////////////////////////////////////////////////
// TPhantomFlagStorageProcessor
////////////////////////////////////////////////////////////////////////////
class TPhantomFlagStorageProcessor : public TActorBootstrapped<TPhantomFlagStorageProcessor> {
public:
    TPhantomFlagStorageProcessor(TPhantomFlagStorageData&& data,
            TPhantomFlagStorageProcessorContext&& ctx)
        : Ctx(std::move(ctx))
        , Data(std::move(data))
        , PendingRead(Ctx.SyncLogCtx->VCtx->Top->GType)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PHANTOM_FLAG_STORAGE_WRITER;
    }

    void Bootstrap() {
        Send(Ctx.ChunkKeeperId, new TEvChunkKeeperDiscover(SubsystemId));
        Become(&TThis::StateInit);
    }

private:
    //////////////////////////////////////////////////////////////////////
    // State functions
    //////////////////////////////////////////////////////////////////////
    STRICT_STFUNC(StateInit,
        hFunc(TEvChunkKeeperDiscoverResult, HandleInit)
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
    )

    STRICT_STFUNC(StateWork,
        hFunc(TEvChunkKeeperAllocateResult, Handle)
        hFunc(TEvChunkKeeperFreeResult, Handle)
        hFunc(NPDisk::TEvChunkWriteResult, Handle)
        hFunc(NPDisk::TEvChunkReadResult, Handle)
        hFunc(TEvPhantomFlagStorageWriteItems, Handle)
        hFunc(TEvPhantomFlagStorageGetSnapshot, Handle)
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
    )

    PDISK_TERMINATE_STATE_FUNC_DEF;

    //////////////////////////////////////////////////////////////////////
    // Handlers
    //////////////////////////////////////////////////////////////////////
    void HandleInit(const TEvChunkKeeperDiscoverResult::TPtr& ev) {
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            std::unordered_set<ui32> discoveredChunks;
            for (const auto& [chunkIdx, chunk] : ev->Get()->Chunks) {
                discoveredChunks.insert(chunkIdx);
                if (!Data.Chunks.contains(chunkIdx)) {
                    // chunk was allocated but wasn't committed to SyncLog
                    // entryPoint before crash
                    Data.Chunks[chunkIdx] = TPhantomFlagStorageData::TChunk{
                        .DataSize = 0,
                    };
                }
            }

            std::unordered_map<ui32, TPhantomFlagStorageData::TChunk> chunks;
            for (const auto& [chunkIdx, chunk] : Data.Chunks) {
                if (discoveredChunks.contains(chunkIdx)) {
                    chunks[chunkIdx] = chunk;
                } else {
                    EnqueueChunkDeletion(chunkIdx);
                }
            }
            std::exchange(Data.Chunks, chunks); // filter out deallocated chunks
            SelectTailChunk();
            break;
        }
        default: {
            // ChunkKeeper was disabled, terminate this actor
            PassAway();
            return;
        }
        }

        Become(&TThis::StateWork);
        ProcessQueues();
    }

    void Handle(TEvPhantomFlagStorageWriteItems::TPtr ev) {
        std::ranges::move(ev->Get()->Items.begin(), ev->Get()->Items.end(),
                std::back_inserter(WriteQueue));
        ProcessQueues();
    }

    void Handle(const TEvChunkKeeperAllocateResult::TPtr& ev) {
        RequestInFlight = false;
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            const ui32 chunkIdx = *ev->Get()->ChunkIdx;
            Data.Chunks[chunkIdx] = TPhantomFlagStorageData::TChunk{
                .DataSize = 0,
            };
            TailChunkIdx = chunkIdx;
            TailAvailableSize = Data.ChunkSize;
            ProcessQueues();
            break;
        }
        default:
            // retry
            AllocateNewChunk();
            return;
        }
    }

    void Handle(const TEvChunkKeeperFreeResult::TPtr& ev) {
        RequestInFlight = false;
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            Data.Chunks.erase(ev->Get()->ChunkIdx);
            break;
        }
        default:
            // retry
            DeleteChunk(ev->Get()->ChunkIdx);
            return;
        }
    }

    void Handle(const NPDisk::TEvChunkWriteResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(Ctx.SyncLogCtx->VCtx, ev, TActivationContext::AsActorContext());
        RequestInFlight = false;
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        ui32 bufferSize = ev->Get()->PartsPtr->ByteSize();
        Data.Chunks[chunkIdx].DataSize += bufferSize;
        if (chunkIdx == TailChunkIdx) {
            TailAvailableSize -= bufferSize;
        }
        CommitState();
        ProcessQueues();
    }

    void Handle(const NPDisk::TEvChunkReadResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(Ctx.SyncLogCtx->VCtx, ev, TActivationContext::AsActorContext());
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        PendingRead.ChunksToRead.erase(chunkIdx);
        ProcessReadBuffer(ev->Get()->Data, chunkIdx);
        if (PendingRead.ChunksToRead.empty()) {
            FinalizeRead();
            ProcessQueues();
        }
    }

    void Handle(const TEvPhantomFlagStorageGetSnapshot::TPtr& ev) {
        EnqueueGetSnapshot(ev->Sender);
    }

    //////////////////////////////////////////////////////////////////////
    // Other methods
    //////////////////////////////////////////////////////////////////////

    void SelectTailChunk() {
        TailChunkIdx.reset();
        TailAvailableSize = 0;
        for (const auto& [chunkIdx, chunk] : Data.Chunks) {
            if (Data.ChunkSize - chunk.DataSize > TailAvailableSize) {
                TailChunkIdx.emplace(chunkIdx);
                TailAvailableSize = Data.ChunkSize - chunk.DataSize;
            }
        }
    }

    void EnqueueChunkDeletion(ui32 chunkIdx) {
        RequestQueue.emplace_back(TDeleteChunk{chunkIdx});
    }
    
    void EnqueueGetSnapshot(TActorId requester) {
        RequestQueue.emplace_front(TGetSnapshot{requester});
    }

    void ProcessQueues() {
        while (!RequestInFlight && !RequestQueue.empty()) {
            TRequest request = RequestQueue.front();
            RequestQueue.pop_front();
            RequestInFlight = true;
            std::visit(TOverloaded{
                [&](const std::monostate&) {},
                [&](const TGetSnapshot& req) { GetSnapshot(req.Requester); },
                [&](const TDeleteChunk& req) { DeleteChunk(req.ChunkIdx); },
            }, request);
            return;
        }
        ProcessWriteQueue();
    }

    void ProcessWriteQueue() {
        if (RequestInFlight) {
            return;
        }

        if (!TailChunkIdx || TailAvailableSize < sizeof(TPhantomFlagStorageItem)) {
            AllocateNewChunk();
            return;
        }

        while (!WriteQueue.empty()) {
            const TPhantomFlagStorageItem& item = WriteQueue.front();
            if (TailAvailableSize < PendingWrite.size() + item.SerializedSize()) {
                break;
            }
            item.Serialize(&PendingWrite);
            WriteQueue.pop_front();
        }

        if (!PendingWrite.empty()) {
            IssueWrite();
        }
    }

    void DeleteChunk(ui32 chunkIdx) {
        if (!Data.Chunks.contains(chunkIdx)) {
            RequestInFlight = false;
        } else {
            Send(Ctx.ChunkKeeperId, new TEvChunkKeeperFree(chunkIdx, SubsystemId));
        }
    }

    void GetSnapshot(TActorId requester) {
        RequestInFlight = true;
        PendingRead.Reset(requester);
        for (const auto [chunkIdx, chunk] : Data.Chunks) {
            if (chunk.DataSize > 0) {
                Send(Ctx.SyncLogCtx->PDiskCtx->PDiskId,
                        new NPDisk::TEvChunkRead(Ctx.SyncLogCtx->PDiskCtx->Dsk->Owner, Ctx.SyncLogCtx->PDiskCtx->Dsk->OwnerRound,
                                                 chunkIdx, 0, chunk.DataSize, NPriWrite::SyncLog, nullptr));
                PendingRead.ChunksToRead.insert(chunkIdx);
            }
        }
        if (PendingRead.ChunksToRead.empty()) {
            FinalizeRead();
        }
    }

    void FinalizeRead() {
        RequestInFlight = false;
        Send(PendingRead.Requester, new TEvPhantomFlagStorageGetSnapshotResult(
                TPhantomFlagStorageSnapshot(std::move(PendingRead.Flags),
                                            std::move(PendingRead.Thresholds))));
    }

    void AllocateNewChunk() {
        RequestInFlight = true;
        Send(Ctx.ChunkKeeperId, new TEvChunkKeeperAllocate(SubsystemId));
    }

    void CommitState() {
        Send(Ctx.SyncLogKeeperId, new TEvPhantomFlagStorageCommitData(Data));
    }

    void IssueWrite() {
        Y_DEBUG_ABORT_UNLESS(TailChunkIdx);
        RequestInFlight = true;
        ui32 offset = Data.Chunks[*TailChunkIdx].DataSize;
        auto parts = MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(std::move(PendingWrite));
        Send(Ctx.SyncLogCtx->PDiskCtx->PDiskId,
             new NPDisk::TEvChunkWrite(Ctx.SyncLogCtx->PDiskCtx->Dsk->Owner, Ctx.SyncLogCtx->PDiskCtx->Dsk->OwnerRound,
                                       *TailChunkIdx, offset, parts, nullptr, true, NPriWrite::SyncLog));
    }

    void ProcessReadBuffer(class TBufferWithGaps& bufferWithGaps, ui32 chunkIdx) {
        if (!bufferWithGaps.IsReadable()) {
            return;
        }
        TRcBuf buffer = bufferWithGaps.ToString();
        ui64 offset = 0;
        while (offset < Data.Chunks[chunkIdx].DataSize) {
            TPhantomFlagStorageItem item = TPhantomFlagStorageItem::DeserializeFromRaw(
                    buffer.Data() + offset);
            switch (item.GetType()) {
            case EPhantomFlagStorageItem::Flag:
                PendingRead.Flags.push_back(item.GetFlag().Record);
                break;
            case EPhantomFlagStorageItem::Threshold: {
                TPhantomFlagStorageItem::TThreshold threshold = item.GetThreshold();
                PendingRead.Thresholds.AddBlob(threshold.OrderNumber, threshold.TabletId,
                        threshold.Channel, threshold.Generation, threshold.Step);
                break;
            }
            case EPhantomFlagStorageItem::Unknown:
                break;
            }

            offset += item.SerializedSize();
            if (item.SerializedSize() == 0) {
                return;
            }
        }
    }

    void HandlePoison(const TEvents::TEvPoisonPill::TPtr&, const TActorContext&) {
        PassAway();
    }

private:
    struct TGetSnapshot {
        TActorId Requester;
    };

    struct TDeleteChunk {
        ui32 ChunkIdx;
    };

    using TRequest = std::variant<TGetSnapshot, TDeleteChunk>;

    struct TReaderInfo {
        TReaderInfo(const TBlobStorageGroupType& gtype)
            : Thresholds(gtype)
        {}

        std::unordered_set<ui32> ChunksToRead;
        TPhantomFlags Flags;
        TPhantomFlagThresholds Thresholds;
        TActorId Requester = TActorId{};

        void Reset(TActorId requester) {
            ChunksToRead.clear();
            Flags.clear();
            Thresholds.Clear();
            Requester = requester;
        }
    };

private:
    static constexpr NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem SubsystemId =
            NKikimrVDiskData::TChunkKeeperEntryPoint::PhantomFlagStorage;

    const TPhantomFlagStorageProcessorContext Ctx;
    TPhantomFlagStorageData Data;

    std::deque<TRequest> RequestQueue;
    bool RequestInFlight = false;

    std::deque<TPhantomFlagStorageItem> WriteQueue;
    TString PendingWrite;
    TReaderInfo PendingRead;

    std::optional<ui32> TailChunkIdx;
    ui64 TailAvailableSize = 0;
};

NActors::IActor* CreatePhantomFlagStorageProcessor(TPhantomFlagStorageData&& data,
        TPhantomFlagStorageProcessorContext&& ctx) {
    return new TPhantomFlagStorageProcessor(std::move(data), std::move(ctx));
}

}  // namespace NKikimr::NSyncLog
