#pragma once

#include "blobstorage_hullwritesst.h"
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_fullsyncsst_add.h>

namespace NKikimr {

template <class TKey, class TMemRec>
class TIndexSstWriter {
    using TRec = TIndexRecord<TKey, TMemRec>;
    using TLevelSegment = TLevelSegment<TKey, TMemRec>;
    using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
    using TEvAddFullSyncSsts = TEvAddFullSyncSsts<TKey, TMemRec>;

    TVDiskContextPtr VCtx;
    TPDiskCtxPtr PDiskCtx;
    TIntrusivePtr<TLevelIndex<TKey, TMemRec>> LevelIndex;
    TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& MsgQueue;

    TVector<ui32> Chunks;
    TVector<TLevelSegmentPtr> LevelSegments;

    std::unique_ptr<TBufferedChunkWriter> Writer;
    ui32 Items = 0;
    ui32 ChunkIdx = 0;
    ui64 SstId = 0;
    TRec::TVec PostponedRecs;

    TTrackableVector<TRec> Recs;
    TLevelSegmentPtr LevelSegment;

    void PutPlaceHolder() {
        auto& info = LevelSegment->Info;
        info.FirstLsn = 0;
        info.LastLsn = 0;
        info.InplaceDataTotalSize = 0;
        info.HugeDataTotalSize = 0;
        info.IdxTotalSize = sizeof(TRec) * Items;
        info.Chunks = 1;
        info.IndexParts = 1;
        info.Items = Items;
        info.ItemsWithInplacedData = 0;
        info.ItemsWithHugeData = 0;
        info.OutboundItems = 0;
        info.CTime = TAppData::TimeProvider->Now();

        if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
            LevelSegment->LoadLinearIndex(Recs);
            Recs.clear();
        } else {
            Recs.shrink_to_fit();
            LevelSegment->LoadedIndex = std::move(Recs);
        }

        TVector<ui32> usedChunks;
        usedChunks.push_back(ChunkIdx);
        LevelSegment->AllChunks = std::move(usedChunks);

        LevelSegment->AssignedSstId = SstId;

        auto ratio = MakeIntrusive<NHullComp::TSstRatio>();
        ratio->IndexItemsTotal = ratio->IndexItemsKeep = info.Items;
        ratio->IndexBytesTotal = ratio->IndexBytesKeep = info.IdxTotalSize;
        ratio->InplacedDataTotal = ratio->InplacedDataKeep = 0;
        ratio->HugeDataTotal = ratio->HugeDataKeep = 0;
        LevelSegment->StorageRatio.Set(ratio);

        TIdxDiskPlaceHolder placeHolder(SstId);
        placeHolder.Info = info;
        placeHolder.PrevPart = {};

        LevelSegment->LastPartAddr = TDiskPart(ChunkIdx, 0, info.IdxTotalSize + sizeof(TIdxDiskPlaceHolder));
        LevelSegment->IndexParts.push_back(LevelSegment->LastPartAddr);
        Writer->Push(&placeHolder, sizeof(placeHolder));
    }

    void FinishChunk() {
        PutPlaceHolder();
        Writer->FinishChunk();
        Writer.reset();

        LevelSegments.push_back(std::move(LevelSegment));
        Chunks.push_back(ChunkIdx);
    }

public:
    TIndexSstWriter(
            TVDiskContextPtr vCtx,
            TPDiskCtxPtr pDiskCtx,
            TIntrusivePtr<TLevelIndex<TKey, TMemRec>> levelIndex,
            TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& msgQueue)
        : VCtx(std::move(vCtx))
        , PDiskCtx(std::move(pDiskCtx))
        , LevelIndex(levelIndex)
        , MsgQueue(msgQueue)
        , Recs(TMemoryConsumer(VCtx->SstIndex))
    {}

    bool Push(const TIndexRecord<TKey, TMemRec>::TVec& records) {
        if (!Writer) {
            PostponedRecs.insert(PostponedRecs.end(), records.begin(), records.end());
            return false;
        }

        auto freeSpace = Writer->GetFreeSpace();
        auto recsSize = sizeof(TRec) * records.size();

        if (recsSize + sizeof(TIdxDiskPlaceHolder) <= freeSpace) {
            Recs.insert(Recs.end(), records.begin(), records.end());
            Writer->Push(records.begin(), recsSize);
            Items += records.size();
            return true;
        }

        ui32 recsFit = (freeSpace - sizeof(TIdxDiskPlaceHolder)) / sizeof(TRec);
        Writer->Push(records.begin(), sizeof(TRec) * recsFit);
        Items += recsFit;
        auto it = records.begin() + recsFit;
        Recs.insert(Recs.end(), records.begin(), it);
        PostponedRecs.insert(PostponedRecs.end(), it, records.end());

        FinishChunk();
        return false;
    }

    void OnChunkReserved(ui32 chunkIdx) {
        Writer = std::make_unique<TBufferedChunkWriter>(
            TMemoryConsumer(VCtx->SstIndex),
            PDiskCtx->Dsk->Owner,
            PDiskCtx->Dsk->OwnerRound,
            NPriWrite::SyncLog, // ???
            (ui32)PDiskCtx->Dsk->ChunkSize,
            PDiskCtx->Dsk->AppendBlockSize,
            (ui32)PDiskCtx->Dsk->BulkWriteBlockSize,
            chunkIdx,
            MsgQueue);

        Recs.reserve((PDiskCtx->Dsk->ChunkSize - sizeof(TIdxDiskPlaceHolder)) / sizeof(TRec));
        LevelSegment = MakeIntrusive<TLevelSegment>(VCtx);

        ChunkIdx = chunkIdx;
        SstId = LevelIndex->AllocSstId();

        // TEvLocalSyncData buffers must be always less than chunk size
        bool ok = Push(PostponedRecs);
        Y_VERIFY_S(ok, VCtx->VDiskLogPrefix);
        PostponedRecs.clear();
    }

    void Finish() {
        if (Writer) {
            FinishChunk();
        }
    }

    TActorId GetLevelIndexActorId() const {
        return LevelIndex->LIActor;
    }

    std::unique_ptr<TEvAddFullSyncSsts> GenerateCommitMessage(const TActorId sstWriterId) {
        if (!Writer) {
            return {};
        }
        auto msg = std::make_unique<TEvAddFullSyncSsts>();
        msg->CommitChunks = std::move(Chunks);
        msg->LevelSegments = std::move(LevelSegments);
        msg->SstWriterId = sstWriterId;
        return std::move(msg);
    }
};

} // NKikimr
