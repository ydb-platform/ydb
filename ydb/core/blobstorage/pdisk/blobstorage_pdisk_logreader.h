#pragma once
#include "defs.h"

#include "blobstorage_pdisk_state.h"
#include "blobstorage_pdisk_logreader_base.h"

namespace NKikimr {
namespace NPDisk {

class TCompletionLogReadPart : public TCompletionAction {
private:
    TIntrusivePtr<TLogReaderBase> Reader;
    ui64 Offset;

public:
    TCompletionLogReadPart(const TIntrusivePtr<TLogReaderBase> &reader,
            ui64 offset)
        : Reader(reader)
        , Offset(offset)
    {}

    void Exec(TActorSystem *actorSystem) override {
        Reader->Exec(Offset, BadOffsets, actorSystem);
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        // Notifies log reader of the log reading error.
        Reader->NotifyError(Offset, ErrorReason);
        delete this;
    }
};

class TPDisk;

struct TLogChunkItem {
    TChunkIdx ChunkIdx;
    bool IsPreviousChunkDropped;
    bool IsPreviousChunkCut;;

    TLogChunkItem(TChunkIdx chunkIdx, bool isPreviousChunkDropped, bool isPreviousChunkCut)
        : ChunkIdx(chunkIdx)
        , IsPreviousChunkDropped(isPreviousChunkDropped)
        , IsPreviousChunkCut(isPreviousChunkCut)
    {}
};

class TLogReader : public TLogReaderBase {
    struct TSectorData;
    class TDoubleBuffer;

    enum class ELogReaderState {
        PrepareToRead,
        NewLogChunk,
        ScheduleForwardReads,
        ProcessAlreadyReadSectors,
        TryProceedNextChunk,
    };

    bool IsInitial;
    TPDisk * const PDisk;
    std::shared_ptr<TPDiskCtx> PCtx;
    const TActorId ReplyTo;

    TOwner Owner;
    TLogPosition OwnerLogStartPosition;
    TLogPosition Position;
    ui64 SizeLimit;
    THolder<TEvReadLogResult> Result;
    TLogChunkInfo *ChunkInfo;

    const ui32 BufferSizeSectors;
    THolder<TDoubleBuffer> Sector;

    THolder<TMap<ui32, TChunkState>> ChunkOwnerMap;
    ELogReaderState State;
    std::atomic<bool> IsReplied;

    TLogPosition LastGoodToWriteLogPosition;
    ui64 MaxNonce;
    ui64 LastNonce;
    ui64 LastDataNonce;
    bool OnEndOfSplice;
    TPDiskStreamCypher Cypher;
    ui32 OffsetInSector;
    bool SetLastGoodToWritePosition;
    ui32 ChunkIdx;
    ui64 SectorIdx;
    bool IsLastRecordHeaderValid;
    ui64 FirstLsnToKeep;
    ui64 FirstNonceToKeep;
    TVDiskID OwnerVDiskId;
    bool IsLastRecordSkipped;
    ui64 ResultSize;
    TLogRecordHeader LastRecordHeader;
    ui64 LastRecordHeaderNonce;
    TString LastRecordData;
    ui32 LastRecordDataWritePosition;
    ui64 EndSectorIdx;
    ui64 SectorsToSkip;
    ui64 ExpectedOffset;
    const ui32 LogEndChunkIdx;
    const ui64 LogEndSectorIdx;
    TReqId ReqId;
    TVector<TLogChunkItem> ChunksToRead;
    TVector<TLogChunkItem>::iterator CurrentChunkToRead;
    TVector<ui64> BadOffsets;
    TMutex ExecMutex;

    ui32 ErasurePartCount;
    bool ParseCommits;

public:
    TLogReader(bool isInitial, TPDisk *pDisk, TActorSystem * const actorSystem, const TActorId &replyTo, TOwner owner,
            TLogPosition ownerLogStartPosition, EOwnerGroupType ownerGroupType, TLogPosition position, ui64 sizeLimit,
            ui64 lastNonce, ui32 logEndChunkIdx, ui64 logEndSectorIdx, TReqId reqId,
            TVector<TLogChunkItem> &&chunksToRead, ui64 firstLsnToKeep, ui64 firstNonceToKeep, TVDiskID ownerVDiskId);

    virtual ~TLogReader();

    void Exec(ui64 offsetRead, TVector<ui64> &badOffsets, TActorSystem *actorSystem) override;
    void NotifyError(ui64 offsetRead, TString& errorReason) override;

private:
    TString SelfInfo();
    bool PrepareToRead();
    void ScheduleForward(ui64 offset);
    ui64 BufferIdxFromOffset(ui64 offset);
    void UpdateLastGoodToWritePosition();
    void LogRawData(ui8* data, ui64 size, TString info);
    void ProcessLogPageTerminator(ui8 *data, ui32 sectorPayloadSize);
    void ProcessLogPageNonceJump2(ui8 *data, const ui64 previousNonce, const ui64 previousDataNonce);
    void ProcessLogPageNonceJump1(ui8 *data, const ui64 previousNonce);
    bool ProcessSectorSet(TSectorData *sector);
    void ReplyOk();
    void ReplyOkInTheMiddle();
    void ReplyError();
    void Reply();
    bool GetIsReplied() const override;
    bool ProcessNextChunkReference(TSectorData& sector);
    void UpdateNewChunkInfo(ui32 currChunk, const TMaybe<ui32> prevChunkIdx);
    void SwitchToChunk(ui32 chunkIdx);
    void ScheduleReadAsync(TSectorData &sector, ui32 sectorsToRead);
    bool RegisterBadOffsets(TVector<ui64> &badOffsets);
    void ReleaseUsedBadOffsets();
};

} // NPDisk
} // NKikimr

