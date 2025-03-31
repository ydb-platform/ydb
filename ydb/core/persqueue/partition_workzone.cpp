#include "partition_workzone.h"
#include "partition_util.h"

namespace NKikimr::NPQ {

TPartitionWorkZone::TPartitionWorkZone(const TPartitionId& partition)
    : PartitionedBlob(partition, 0, "", 0, 0, 0, Head, NewHead, true, false, 8_MB)
    , NewHeadKey{TKey{}, 0, TInstant::Zero(), 0}
    , BodySize(0)
    , MaxWriteResponsesSize(0)
{
}

void TPartitionWorkZone::CheckHeadConsistency(const TVector<ui32>& compactLevelBorder,
                                              const ui32 totalLevels,
                                              const ui32 totalMaxCount) const
{
    ui32 p = 0;
    for (ui32 j = 0; j < DataKeysHead.size(); ++j) {
        ui32 s = 0;
        for (ui32 k = 0; k < DataKeysHead[j].KeysCount(); ++k) {
            Y_ABORT_UNLESS(p < HeadKeys.size());
            Y_ABORT_UNLESS(DataKeysHead[j].GetKey(k) == HeadKeys[p].Key);
            Y_ABORT_UNLESS(DataKeysHead[j].GetSize(k) == HeadKeys[p].Size);
            s += DataKeysHead[j].GetSize(k);
            Y_ABORT_UNLESS(j + 1 == totalLevels || DataKeysHead[j].GetSize(k) >= compactLevelBorder[j + 1]);
            ++p;
        }
        Y_ABORT_UNLESS(s < DataKeysHead[j].Border());
    }
    Y_ABORT_UNLESS(DataKeysBody.empty() ||
             Head.Offset >= DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());
    Y_ABORT_UNLESS(p == HeadKeys.size());
    if (!HeadKeys.empty()) {
        Y_ABORT_UNLESS(HeadKeys.size() <= totalMaxCount);
        Y_ABORT_UNLESS(HeadKeys.front().Key.GetOffset() == Head.Offset);
        Y_ABORT_UNLESS(HeadKeys.front().Key.GetPartNo() == Head.PartNo);
        for (p = 1; p < HeadKeys.size(); ++p) {
            Y_ABORT_UNLESS(HeadKeys[p].Key.GetOffset() == HeadKeys[p-1].Key.GetOffset() + HeadKeys[p-1].Key.GetCount());
            Y_ABORT_UNLESS(HeadKeys[p].Key.ToString() > HeadKeys[p-1].Key.ToString());
        }
    }
}

bool TPartitionWorkZone::PositionInBody(ui64 offset, ui32 partNo) const
{
    return offset < Head.Offset || ((Head.Offset == offset) && (partNo < Head.PartNo));
}

bool TPartitionWorkZone::PositionInHead(ui64 offset, ui32 partNo) const
{
    return Head.Offset < offset || ((Head.Offset == offset) && (Head.PartNo < partNo));
}

void TPartitionWorkZone::NewPartitionedBlob(const TPartitionId& partitionId,
                                            const ui64 offset,
                                            const TString& sourceId,
                                            const ui64 seqNo,
                                            const ui16 totalParts,
                                            const ui32 totalSize,
                                            bool headCleared,
                                            bool needCompactHead,
                                            const ui32 maxBlobSize,
                                            ui16 nextPartNo)
{
    PartitionedBlob = TPartitionedBlob(partitionId,
                                       offset,
                                       sourceId,
                                       seqNo,
                                       totalParts,
                                       totalSize,
                                       Head,
                                       NewHead,
                                       headCleared,
                                       needCompactHead,
                                       maxBlobSize,
                                       nextPartNo);
}

void TPartitionWorkZone::ClearPartitionedBlob(const TPartitionId& partitionId, ui32 maxBlobSize)
{
    PartitionedBlob = TPartitionedBlob(partitionId,
                                       0,
                                       "",
                                       0,
                                       0,
                                       0,
                                       Head,
                                       NewHead,
                                       true,
                                       false,
                                       maxBlobSize);
}

void TPartitionWorkZone::SyncHeadKeys()
{
    if (!CompactedKeys.empty()) {
        HeadKeys.clear();
    }
}

void TPartitionWorkZone::SyncNewHeadKey()
{
    if (NewHeadKey.Size <= 0) {
        return;
    }

    auto isLess = [](const TKey& lhs, const TKey& rhs) {
        if (lhs.GetOffset() < rhs.GetOffset()) {
            return true;
        }
        if (lhs.GetOffset() == rhs.GetOffset()) {
            return lhs.GetPartNo() < rhs.GetPartNo();
        }
        return false;
    };

    while (!HeadKeys.empty() && !isLess(HeadKeys.back().Key, NewHeadKey.Key)) {
        // HeadKeys.back >= NewHeadKey
        HeadKeys.pop_back();
    }

    HeadKeys.push_back(std::move(NewHeadKey));

    NewHeadKey = TDataKey{TKey{}, 0, TInstant::Zero(), 0};
}

void TPartitionWorkZone::SyncDataKeysBody(TInstant now,
                                          TBlobKeyTokenCreator makeBlobKeyToken,
                                          ui64& startOffset,
                                          std::deque<std::pair<ui64, ui64>>& gapOffsets,
                                          ui64& gapSize)
{
    auto getNextOffset = [](const TKey& k) {
        return k.GetOffset() + k.GetCount();
    };
    auto getDataSize = [](const TDataKey& k) {
        return k.CumulativeSize + k.Size;
    };

    while (!CompactedKeys.empty()) {
        const auto& [key, blobSize] = CompactedKeys.front();
        Y_ABORT_UNLESS(!key.HasSuffix());

        BodySize += blobSize;

        ui64 lastOffset = DataKeysBody.empty() ? 0 : getNextOffset(DataKeysBody.back().Key);
        Y_ABORT_UNLESS(lastOffset <= key.GetOffset());

        if (DataKeysBody.empty()) {
            startOffset = key.GetOffset() + (key.GetPartNo() > 0 ? 1 : 0);
        } else if (lastOffset < key.GetOffset()) {
            gapOffsets.emplace_back(lastOffset, key.GetOffset());
            gapSize += key.GetOffset() - lastOffset;
        }

        DataKeysBody.emplace_back(key,
                                  blobSize,
                                  now,
                                  DataKeysBody.empty() ? 0 : getDataSize(DataKeysBody.back()),
                                  makeBlobKeyToken(key.ToString()));

        CompactedKeys.pop_front();
    } // head cleared, all data moved to body
}

}
