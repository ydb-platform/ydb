#pragma once

#include "blobstorage_hullwritesst.h"
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_fullsyncsst_add.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr {

template <class TKey, class TMemRec>
class TIndexSstWriterBase {
    using TRec = TIndexRecord<TKey, TMemRec>;
    using TLevelSegment = TLevelSegment<TKey, TMemRec>;
    using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
    using TEvAddFullSyncSsts = TEvAddFullSyncSsts<TKey, TMemRec>;

protected:
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

        LevelSegments.push_back(std::move(LevelSegment));
        Chunks.push_back(ChunkIdx);
    }

    void CreateWriter() {
        Writer = std::make_unique<TBufferedChunkWriter>(
            TMemoryConsumer(VCtx->SstIndex),
            PDiskCtx->Dsk->Owner,
            PDiskCtx->Dsk->OwnerRound,
            NPriWrite::SyncLog, // ???
            (ui32)PDiskCtx->Dsk->ChunkSize,
            PDiskCtx->Dsk->AppendBlockSize,
            (ui32)PDiskCtx->Dsk->BulkWriteBlockSize,
            ChunkIdx,
            MsgQueue);
    }

public:
    TIndexSstWriterBase(
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

    TActorId GetLevelIndexActorId() const {
        return LevelIndex->LIActor;
    }

    std::unique_ptr<TEvAddFullSyncSsts> GenerateCommitMessage(const TActorId sstWriterId) {
        STLOG(PRI_DEBUG, BS_SYNCER, BSFS24, VDISKP(VCtx->VDiskLogPrefix,
            "TIndexSstWriterBase: GenerateCommitMessage"),
            (ChunkCount, Chunks.size()), (RecordSize, sizeof(TRec)));

        if (Chunks.empty()) {
            return {};
        }

        auto msg = std::make_unique<TEvAddFullSyncSsts>();
        msg->CommitChunks = std::move(Chunks);
        msg->LevelSegments = std::move(LevelSegments);
        msg->SstWriterId = sstWriterId;
        return std::move(msg);
    }
};

template <class TKey, class TMemRec>
class TIndexSstWriter : public TIndexSstWriterBase<TKey, TMemRec> {
    using TRec = TIndexRecord<TKey, TMemRec>;
    using TLevelSegment = TLevelSegment<TKey, TMemRec>;
    using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
    using TEvAddFullSyncSsts = TEvAddFullSyncSsts<TKey, TMemRec>;

public:
    TIndexSstWriter(
            TVDiskContextPtr vCtx,
            TPDiskCtxPtr pDiskCtx,
            TIntrusivePtr<TLevelIndex<TKey, TMemRec>> levelIndex,
            TQueue<std::unique_ptr<NPDisk::TEvChunkWrite>>& msgQueue)
        : TIndexSstWriterBase<TKey, TMemRec>(vCtx, pDiskCtx, levelIndex, msgQueue)
    {}

    bool PushRecord(const TKey& key, const TMemRec& memRec) {
        TRec rec(key, memRec);

        if (!this->Writer) {
            this->PostponedRecs.push_back(rec);
            return false;
        }

        if (sizeof(TRec) + sizeof(TIdxDiskPlaceHolder) <= this->Writer->GetFreeSpace()) {
            this->Recs.push_back(rec);
            this->Writer->Push(&rec, sizeof(TRec));
            ++this->Items;
            return true;
        }

        this->PostponedRecs.push_back(rec);
        this->FinishChunk();
        return false;
    }

    bool Push(const TRec::TVec& records) {
        STLOG(PRI_DEBUG, BS_SYNCER, BSFS20, VDISKP(this->VCtx->VDiskLogPrefix,
            "TIndexSstWriter: Push"),
            (RecordCount, records.size()), (RecordSize, sizeof(TRec)));

        if (!this->Writer) {
            this->PostponedRecs.insert(this->PostponedRecs.end(), records.begin(), records.end());
            return false;
        }

        auto freeSpace = this->Writer->GetFreeSpace();
        auto recsSize = sizeof(TRec) * records.size();

        STLOG(PRI_DEBUG, BS_SYNCER, BSFS21, VDISKP(this->VCtx->VDiskLogPrefix,
            "TIndexSstWriter: Push"),
            (FreeSpace, freeSpace), (Size, recsSize), (RecordSize, sizeof(TRec)));

        if (recsSize + sizeof(TIdxDiskPlaceHolder) <= freeSpace) {
            this->Recs.insert(this->Recs.end(), records.begin(), records.end());
            this->Writer->Push(records.begin(), recsSize);
            this->Items += records.size();
            return true;
        }

        ui32 recsFit = (freeSpace - sizeof(TIdxDiskPlaceHolder)) / sizeof(TRec);
        this->Writer->Push(records.begin(), sizeof(TRec) * recsFit);
        this->Items += recsFit;
        auto it = records.begin() + recsFit;
        this->Recs.insert(this->Recs.end(), records.begin(), it);
        this->PostponedRecs.insert(this->PostponedRecs.end(), it, records.end());

        this->FinishChunk();
        return false;
    }

    void OnChunkReserved(ui32 chunkIdx) {
        STLOG(PRI_DEBUG, BS_SYNCER, BSFS22, VDISKP(this->VCtx->VDiskLogPrefix,
            "TIndexSstWriter: OnChunkReserved"),
            (ChunkIdx, chunkIdx), (RecordSize, sizeof(TRec)));

        this->Items = 0;
        this->ChunkIdx = chunkIdx;
        this->SstId = this->LevelIndex->AllocSstId();

        this->CreateWriter();

        this->Recs.reserve((this->PDiskCtx->Dsk->ChunkSize - sizeof(TIdxDiskPlaceHolder)) / sizeof(TRec));
        this->LevelSegment = MakeIntrusive<TLevelSegment>(this->VCtx);

        // TEvLocalSyncData buffers must be always less than chunk size
        bool ok = this->Push(this->PostponedRecs);
        Y_VERIFY_S(ok, this->VCtx->VDiskLogPrefix);
        this->PostponedRecs.clear();
    }

    void Finish() {
        STLOG(PRI_DEBUG, BS_SYNCER, BSFS23, VDISKP(this->VCtx->VDiskLogPrefix,
            "TIndexSstWriter: Finish"),
            (RecordSize, sizeof(TRec)));

        if (this->Writer) {
            this->FinishChunk();
        }
    }
};

} // NKikimr
