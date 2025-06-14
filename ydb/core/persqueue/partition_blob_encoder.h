#pragma once

#include "blob.h"
#include "key.h"
#include "events/internal.h"

namespace NKikimr::NPQ {

class TKeyLevel;

struct TPartitionBlobEncoder {
    explicit TPartitionBlobEncoder(const TPartitionId& partition, bool fastWrite);

    void CheckHeadConsistency(const TVector<ui32>& compactLevelBorder,
                              const ui32 totalLevels,
                              const ui32 totalMaxCount) const;

    ui64 GetSize() const;
    ui64 GetBodySizeBefore(TInstant timestamp) const;

    TVector<TRequestedBlob> GetBlobsFromBody(const ui64 startOffset,
                                             const ui16 partNo,
                                             const ui32 maxCount,
                                             const ui32 maxSize,
                                             ui32& count,
                                             ui32& size,
                                             ui64 lastOffset,
                                             TBlobKeyTokens* blobKeyTokens) const;
    TVector<TClientBlob> GetBlobsFromHead(const ui64 startOffset,
                                          const ui16 partNo,
                                          const ui32 maxCount,
                                          const ui32 maxSize,
                                          const ui64 readTimestampMs,
                                          ui32& rcount,
                                          ui32& rsize,
                                          ui64& insideHeadOffset,
                                          ui64 lastOffset) const;

    ui64 GetHeadGapSize() const;
    ui64 GetSizeLag(i64 offset) const;

    bool PositionInBody(ui64 offset, ui32 partNo) const;
    bool PositionInHead(ui64 offset, ui32 partNo) const;

    bool IsEmpty() const;
    const TDataKey* GetLastKey() const;

    bool IsNothingWritten() const;
    bool IsLastBatchPacked() const;

    TString SerializeForKey(const TKey& key, ui32 size,
                            ui64 endOffset,
                            TInstant& writeTimestamp) const;

    TKey KeyForWrite(TKeyPrefix::EType type, const TPartitionId& partitionId, bool needCompaction) const;
    TKey KeyForFastWrite(TKeyPrefix::EType type, const TPartitionId& partitionId) const;

    void NewPartitionedBlob(const TPartitionId& partition,
                            const ui64 offset,
                            const TString& sourceId,
                            const ui64 seqNo,
                            const ui16 totalParts,
                            const ui32 totalSize,
                            bool headCleared,
                            bool needCompactHead,
                            const ui32 maxBlobSize,
                            ui16 nextPartNo = 0);
    void ClearPartitionedBlob(const TPartitionId& partitionId, ui32 maxBlobSize);

    void SyncHeadKeys();
    void SyncNewHeadKey();
    void SyncDataKeysBody(TInstant now,
                          TBlobKeyTokenCreator makeBlobKeyToken,
                          ui64& startOffset,
                          std::deque<std::pair<ui64, ui64>>& gapOffsets,
                          ui64& gapSize);
    void SyncHeadFromNewHead();
    void SyncHead(ui64& startOffset, ui64& endOffset);
    void SyncHeadFastWrite(ui64& startOffset, ui64& endOffset);

    void ResetNewHead(ui64 endOffset);

    void PackLastBatch();

    std::pair<TKey, ui32> Compact(const TKey& key, bool headCleared);

    ui64 StartOffset;
    ui64 EndOffset;

    THead Head;
    THead NewHead;
    TPartitionedBlob PartitionedBlob;
    std::deque<std::pair<TKey, ui32>> CompactedKeys; //key and blob size
    TDataKey NewHeadKey;

    ui64 BodySize;
    ui32 MaxWriteResponsesSize;

    std::deque<TDataKey> DataKeysBody;
    TVector<TKeyLevel> DataKeysHead;
    std::deque<TDataKey> HeadKeys;
    ui64 FirstUncompactedOffset = 0;

    bool HaveData = false;
    bool HeadCleared = false;

    void Dump() const;

    bool ForFastWrite = true;
};

}
