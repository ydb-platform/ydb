#pragma once

#include "blob.h"
#include "key.h"
#include "events/internal.h"

namespace NKikimr::NPQ {

class TKeyLevel;

struct TPartitionWorkZone {
    explicit TPartitionWorkZone(const TPartitionId& partition);

    void CheckHeadConsistency(const TVector<ui32>& compactLevelBorder,
                              const ui32 totalLevels,
                              const ui32 totalMaxCount) const;

    ui64 GetSize() const { return BodySize + Head.PackedSize; }

    bool PositionInBody(ui64 offset, ui32 partNo) const;
    bool PositionInHead(ui64 offset, ui32 partNo) const;

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

    void ResetNewHead(ui64 endOffset);

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
};

}
