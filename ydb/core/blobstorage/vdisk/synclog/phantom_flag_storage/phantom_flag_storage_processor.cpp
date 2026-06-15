#include "phantom_flags.h"
#include "phantom_flag_thresholds.h"
#include "phantom_flag_storage_builder.h"
#include "phantom_flag_storage_processor.h"

#include <util/generic/overloaded.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/chunk_keeper/chunk_keeper_events.h>
#include <ydb/core/util/stlog.h>

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
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP01, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Bootstrap PhantomFlagStorageProcessor"),
                (ChunkCount, Data.Chunks.size()),
                (ChunkSize, Data.ChunkSize));
        Send(Ctx.ChunkKeeperId, new TEvChunkKeeperDiscover(SubsystemId));
        RequestInFlight = true;
        Become(&TThis::StateInit);
    }

private:
    //////////////////////////////////////////////////////////////////////
    // State functions
    //////////////////////////////////////////////////////////////////////
    STRICT_STFUNC(StateInit,
        hFunc(TEvPhantomFlagStorageWriteItems, Handle)
        hFunc(TEvPhantomFlagExtractedFromChunk, Handle)
        hFunc(TEvPhantomFlagStorageDrop, Handle)
        hFunc(TEvChunkKeeperDiscoverResult, HandleInit)
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
    )

    STRICT_STFUNC(StateWork,
        hFunc(TEvPhantomFlagStorageDrop, Handle)
        hFunc(TEvChunkKeeperAllocateResult, Handle)
        hFunc(TEvChunkKeeperFreeResult, Handle)
        hFunc(NPDisk::TEvChunkWriteResult, Handle)
        hFunc(NPDisk::TEvChunkReadResult, Handle)
        hFunc(TEvPhantomFlagStorageWriteItems, Handle)
        hFunc(TEvPhantomFlagExtractedFromChunk, Handle)
        hFunc(TEvPhantomFlagStorageGetSnapshot, Handle)
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
        hFunc(TEvPhantomFlagStorageFinishBuilder, Handle)
    )

    PDISK_TERMINATE_STATE_FUNC_DEF;

    //////////////////////////////////////////////////////////////////////
    // Handlers
    //////////////////////////////////////////////////////////////////////
    void HandleInit(const TEvChunkKeeperDiscoverResult::TPtr& ev) {
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP02, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvChunkKeeperDiscoverResult"),
                (Event, ev->Get()->ToString()));
        RequestInFlight = false;
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
            // ChunkKeeper is disabled, unable to manage chunks, terminate this actor
            PassAway();
            return;
        }
        }

        Become(&TThis::StateWork);
        ProcessQueues();
    }

    void Handle(TEvPhantomFlagStorageWriteItems::TPtr ev) {
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_PROCESSOR, BSPFP03, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvPhantomFlagStorageWriteItems"),
                (ItemCount, ev->Get()->Items.size()));
        TWriteBatch batch{.SourceChunkIdx = std::nullopt};
        std::ranges::move(ev->Get()->Items.begin(), ev->Get()->Items.end(),
                std::back_inserter(batch.Items));
        BatchQueue.push_back(std::move(batch));
        ProcessQueues();
    }

    void Handle(TEvPhantomFlagExtractedFromChunk::TPtr ev) {
        const ui32 chunkIdx = ev->Get()->ChunkIdx;
        auto& flags = ev->Get()->Flags;
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP12, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvPhantomFlagExtractedFromChunk"),
                (SourceChunkIdx, chunkIdx),
                (FlagCount, flags.size()));
        TWriteBatch batch{.SourceChunkIdx = chunkIdx};
        for (const TLogoBlobRec& flag : flags) {
            batch.Items.push_back(TPhantomFlagStorageItem::CreateFlag(&flag));
        }
        BatchQueue.push_back(std::move(batch));
        ProcessQueues();
    }

    void Handle(const TEvChunkKeeperAllocateResult::TPtr& ev) {
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP04, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvChunkKeeperAllocateResult"),
                (Event, ev->Get()->ToString()));
        RequestInFlight = false;
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            const ui32 chunkIdx = *ev->Get()->ChunkIdx;
            Data.Chunks[chunkIdx] = TPhantomFlagStorageData::TChunk{
                .DataSize = 0,
            };
            TailChunkIdx = chunkIdx;
            TailAvailableSize = Data.ChunkSize;
            break;
        }
        default:
            // retry
            AllocateNewChunk();
            return;
        }
        ProcessQueues();
    }

    void Handle(const TEvChunkKeeperFreeResult::TPtr& ev) {
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP05, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvChunkKeeperFreeResult"),
                (Event, ev->Get()->ToString()));
        RequestInFlight = false;
        Data.Chunks.erase(ev->Get()->ChunkIdx);
        CommitState();
        ProcessQueues();
    }

    void Handle(const NPDisk::TEvChunkWriteResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_PROCESSOR, BSPFP06, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvChunkWriteResult"),
                (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(Ctx.SyncLogCtx->VCtx, ev, TActivationContext::AsActorContext());
        RequestInFlight = false;
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        if (chunkIdx == TailChunkIdx) {
            TailAvailableSize -= PendingWriteSize;
        }
        Data.Chunks[chunkIdx].DataSize += std::exchange(PendingWriteSize, 0);
        // Defer CommitState while a batch is still being drained: a partial
        // write must not be reflected in the persisted entry point. The
        // batch's atomic commit happens in FinishActiveBatch.
        if (!ActiveBatch) {
            CommitState();
            ProcessQueues();
        } else {
            ProcessWriteQueue();
        }
    }

    void Handle(const NPDisk::TEvChunkReadResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_PROCESSOR, BSPFP07, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvChunkReadResult"),
                (Event, ev->Get()->ToString()));
        CHECK_PDISK_RESPONSE(Ctx.SyncLogCtx->VCtx, ev, TActivationContext::AsActorContext());
        ui32 chunkIdx = ev->Get()->ChunkIdx;
        PendingRead.ChunksToRead.erase(chunkIdx);
        ProcessReadBuffer(ev->Get()->Data, chunkIdx);
        if (PendingRead.ChunksToRead.empty()) {
            Register(CreatePhantomFlagStorageBuilderActor(Ctx.SyncLogCtx, SelfId(),
                    std::move(PendingRead.SyncLogSnapshot), false));
        }
    }

    void Handle(const TEvPhantomFlagStorageGetSnapshot::TPtr& ev) {
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP08, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvPhantomFlagStorageGetSnapshot"));
        EnqueueGetSnapshot(ev->Sender, std::move(ev->Get()->SyncLogSnapshot));
        ProcessQueues();
    }

    void Handle(const TEvPhantomFlagStorageDrop::TPtr&) {
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP11, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvPhantomFlagStorageDrop"),
                (ChunkCount, Data.Chunks.size()));
        TailChunkIdx = std::nullopt;
        TailAvailableSize = 0;
        BatchQueue.clear();
        ActiveBatch.reset();
        PendingWrite.clear();
        PendingWriteSize = 0;
        CommitState();
        for (const auto& [chunkIdx, chunk] : Data.Chunks) {
            EnqueueChunkDeletion(chunkIdx);
        }
        ProcessQueues();
    }

    void Handle(const TEvPhantomFlagStorageFinishBuilder::TPtr& ev) {
        TPhantomFlags& flags = ev->Get()->Flags;
        STLOG(PRI_INFO, BS_PHANTOM_FLAG_PROCESSOR, BSPFP13, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Handle TEvPhantomFlagStorageFinishBuilder"),
                (FlagCount, flags.size()));
        std::move(flags.begin(), flags.end(), std::back_inserter(PendingRead.Flags));
        FinalizeRead();
    }

    void HandlePoison(const TEvents::TEvPoisonPill::TPtr&, const TActorContext&) {
        PassAway();
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

    void EnqueueGetSnapshot(TActorId requester, TSyncLogSnapshotPtr&& snapshot) {
        RequestQueue.emplace_front(TGetSnapshot{requester, std::move(snapshot)});
    }

    void ProcessQueues() {
        while (!RequestInFlight && !RequestQueue.empty()) {
            TRequest request = RequestQueue.front();
            RequestQueue.pop_front();
            RequestInFlight = true;
            std::visit(TOverloaded{
                [&](const std::monostate&) {},
                [&](TGetSnapshot& req) { GetSnapshot(req.Requester, std::move(req.Snapshot)); },
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

        if (!ActiveBatch) {
            if (BatchQueue.empty()) {
                return;
            }
            ActiveBatch = std::move(BatchQueue.front());
            BatchQueue.pop_front();
        }

        if (!ActiveBatch->Items.empty()) {
            ui32 nextItemSize = ActiveBatch->Items.front().SerializedSize();
            ui32 minRequiredSize = PendingWrite.size() + nextItemSize + Ctx.AppendBlockSize;

            if (TailAvailableSize < minRequiredSize + Ctx.AppendBlockSize) {
                AllocateNewChunk();
                return;
            }
        }

        while (!ActiveBatch->Items.empty()) {
            const TPhantomFlagStorageItem& item = ActiveBatch->Items.front();
            if (TailAvailableSize < PendingWrite.size() + item.SerializedSize() + Ctx.AppendBlockSize) {
                break;
            }
            item.Serialize(&PendingWrite);
            ActiveBatch->Items.pop_front();
        }

        if (!PendingWrite.empty()) {
            IssueWrite();
            return;
        }

        // Active batch is fully serialized AND last write completed; commit
        // atomically (DataSize advance + ChunksToExtract removal).
        FinishActiveBatch();
    }

    void FinishActiveBatch() {
        Y_ABORT_UNLESS(ActiveBatch);
        Y_ABORT_UNLESS(ActiveBatch->Items.empty());
        Y_ABORT_UNLESS(PendingWrite.empty());
        if (ActiveBatch->SourceChunkIdx) {
            PendingRetiredChunks.push_back(*ActiveBatch->SourceChunkIdx);
        }
        ActiveBatch.reset();
        CommitState();
        ProcessQueues();
    }

    void DeleteChunk(ui32 chunkIdx) {
        if (Data.Chunks.contains(chunkIdx)) {
            Send(Ctx.ChunkKeeperId, new TEvChunkKeeperFree(chunkIdx, SubsystemId));
        } else {
            RequestInFlight = false;
        }
    }

    void GetSnapshot(TActorId requester, TSyncLogSnapshotPtr&& snapshot) {
        RequestInFlight = true;
        PendingRead.Reset(requester, std::move(snapshot));
        for (const auto [chunkIdx, chunk] : Data.Chunks) {
            if (chunk.DataSize > 0) {
                Send(Ctx.SyncLogCtx->PDiskCtx->PDiskId,
                        new NPDisk::TEvChunkRead(Ctx.SyncLogCtx->PDiskCtx->Dsk->Owner, Ctx.SyncLogCtx->PDiskCtx->Dsk->OwnerRound,
                                                 chunkIdx, 0, chunk.DataSize, NPriWrite::SyncLog, nullptr));
                PendingRead.ChunksToRead.insert(chunkIdx);
            }
        }
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP10, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Start reading snapshot"),
                (ChunkToReadCount, PendingRead.ChunksToRead.size()));
        if (PendingRead.ChunksToRead.empty()) {
            Register(CreatePhantomFlagStorageBuilderActor(Ctx.SyncLogCtx, SelfId(),
                    std::move(PendingRead.SyncLogSnapshot), false));
        }
    }

    void FinalizeRead() {
        RequestInFlight = false;
        STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_PROCESSOR, BSPFP09, VDISKP(Ctx.SyncLogCtx->VCtx,
                "Send Snapshot"),
                (FlagCount, PendingRead.Flags.size()));
        Send(PendingRead.Requester, new TEvPhantomFlagStorageGetSnapshotResult(
                TPhantomFlagStorageSnapshot(std::move(PendingRead.Flags),
                                            std::move(PendingRead.Thresholds))));
        ProcessQueues();
    }

    void AllocateNewChunk() {
        RequestInFlight = true;
        Send(Ctx.ChunkKeeperId, new TEvChunkKeeperAllocate(SubsystemId));
    }

    void CommitState() {
        Send(Ctx.SyncLogKeeperId, new TEvPhantomFlagStorageCommitData(
                Data, std::exchange(PendingRetiredChunks, {})));
    }

    void IssueWrite() {
        Y_ABORT_UNLESS(TailChunkIdx);
        RequestInFlight = true;
        ui32 offset = Data.Chunks[*TailChunkIdx].DataSize;
        Y_ABORT_UNLESS(offset <= Data.ChunkSize);
        ui32 maxSize = Data.ChunkSize - offset;
        TPhantomFlagStorageItem::AlignWriteBlock(&PendingWrite, Ctx.AppendBlockSize, maxSize);
        PendingWriteSize = PendingWrite.size();
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
            case EPhantomFlagStorageItem::Skip:
            case EPhantomFlagStorageItem::SkipOneByte:
                break;
            }

            offset += item.SerializedSize();
            if (item.SerializedSize() == 0) {
                return;
            }
        }
    }

private:
    struct TGetSnapshot {
        TActorId Requester;
        TSyncLogSnapshotPtr Snapshot;
    };

    struct TDeleteChunk {
        ui32 ChunkIdx;
    };

    using TRequest = std::variant<TGetSnapshot, TDeleteChunk>;

    struct TWriteBatch {
        std::optional<ui32> SourceChunkIdx; // nullopt for the initial build
        std::deque<TPhantomFlagStorageItem> Items;
    };

    struct TReaderInfo {
        TReaderInfo(const TBlobStorageGroupType& gtype)
            : Thresholds(gtype)
        {}

        std::unordered_set<ui32> ChunksToRead;
        TPhantomFlags Flags;
        TPhantomFlagThresholds Thresholds;
        TActorId Requester = TActorId{};
        TSyncLogSnapshotPtr SyncLogSnapshot;

        void Reset(TActorId requester, TSyncLogSnapshotPtr&& snapshot) {
            ChunksToRead.clear();
            Flags.clear();
            Thresholds.Clear();
            Requester = requester;
            SyncLogSnapshot = std::move(snapshot);
        }
    };

private:
    static constexpr NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem SubsystemId =
            NKikimrVDiskData::TChunkKeeperEntryPoint::PhantomFlagStorage;

    const TPhantomFlagStorageProcessorContext Ctx;
    TPhantomFlagStorageData Data;

    std::deque<TRequest> RequestQueue;
    bool RequestInFlight = false;

    std::deque<TWriteBatch> BatchQueue;
    std::optional<TWriteBatch> ActiveBatch;
    std::vector<ui32> PendingRetiredChunks;
    TString PendingWrite;
    ui32 PendingWriteSize = 0;
    TReaderInfo PendingRead;

    std::optional<ui32> TailChunkIdx;
    ui64 TailAvailableSize = 0;
};

NActors::IActor* CreatePhantomFlagStorageProcessor(TPhantomFlagStorageData&& data,
        TPhantomFlagStorageProcessorContext&& ctx) {
    return new TPhantomFlagStorageProcessor(std::move(data), std::move(ctx));
}

}  // namespace NKikimr::NSyncLog
