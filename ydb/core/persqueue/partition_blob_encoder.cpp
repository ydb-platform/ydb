#include "partition_blob_encoder.h"
#include "partition_util.h"

namespace NKikimr::NPQ {

TPartitionBlobEncoder::TPartitionBlobEncoder(const TPartitionId& partition, bool fastWrite)
    : StartOffset(0)
    , EndOffset(0)
    , PartitionedBlob(partition, 0, "", 0, 0, 0, Head, NewHead, true, false, 8_MB)
    , NewHeadKey{TKey{}, 0, TInstant::Zero(), 0}
    , BodySize(0)
    , MaxWriteResponsesSize(0)
    , ForFastWrite(fastWrite)
{
}

void TPartitionBlobEncoder::CheckHeadConsistency(const TVector<ui32>& compactLevelBorder,
                                                 const ui32 totalLevels,
                                                 const ui32 totalMaxCount) const
{
    ui32 p = 0;
    for (ui32 j = 0; j < DataKeysHead.size(); ++j) {
        ui32 s = 0;
        for (ui32 k = 0; k < DataKeysHead[j].KeysCount(); ++k) {
            Y_ABORT_UNLESS(p < HeadKeys.size());
            Y_ABORT_UNLESS(DataKeysHead[j].GetKey(k) == HeadKeys[p].Key,
                           "DataKeysHead[%" PRIu32 "].Key[%" PRIu32 "]=%s, HeadKeys[%" PRIu32 "]=%s",
                           j, k, DataKeysHead[j].GetKey(k).ToString().data(), p, HeadKeys[p].Key.ToString().data());
            Y_ABORT_UNLESS(DataKeysHead[j].GetSize(k) == HeadKeys[p].Size);
            s += DataKeysHead[j].GetSize(k);
            Y_ABORT_UNLESS(j + 1 == totalLevels || DataKeysHead[j].GetSize(k) >= compactLevelBorder[j + 1]);
            ++p;
        }
        Y_ABORT_UNLESS(s < DataKeysHead[j].Border());
    }
    Y_ABORT_UNLESS(DataKeysBody.empty() ||
                   Head.Offset >= DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount(),
                   "DataKeysBody.size=%" PRISZT ", lastKey=%s, head.offset=%" PRIu64,
                   DataKeysBody.size(),
                   DataKeysBody.back().Key.ToString().data(),
                   Head.Offset);
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

ui64 TPartitionBlobEncoder::GetSize() const
{
    return BodySize + Head.PackedSize;
}

ui64 TPartitionBlobEncoder::GetBodySizeBefore(TInstant expirationTimestamp) const
{
    ui64 size = 0;
    for (size_t i = 1; i < DataKeysBody.size() && DataKeysBody[i].Timestamp < expirationTimestamp; ++i) {
        size += DataKeysBody[i].Size;
    }
    return size;
}

TVector<TRequestedBlob> TPartitionBlobEncoder::GetBlobsFromBody(const ui64 startOffset,
                                                                const ui16 partNo,
                                                                const ui32 maxCount,
                                                                const ui32 maxSize,
                                                                ui32& count,
                                                                ui32& size,
                                                                ui64 lastOffset,
                                                                TBlobKeyTokens* blobKeyTokens) const
{
    TVector<TRequestedBlob> blobs;
    if (!DataKeysBody.empty() && PositionInBody(startOffset, partNo)) { //will read smth from body
        auto it = std::upper_bound(DataKeysBody.begin(), DataKeysBody.end(), std::make_pair(startOffset, partNo),
            [](const std::pair<ui64, ui16>& offsetAndPartNo, const TDataKey& p) { return offsetAndPartNo.first < p.Key.GetOffset() || offsetAndPartNo.first == p.Key.GetOffset() && offsetAndPartNo.second < p.Key.GetPartNo();});
        if (it == DataKeysBody.begin()) //could be true if data is deleted or gaps are created
            return blobs;
        Y_ABORT_UNLESS(it != DataKeysBody.begin()); //always greater, startoffset can't be less that StartOffset
        Y_ABORT_UNLESS(it == DataKeysBody.end() || it->Key.GetOffset() > startOffset || it->Key.GetOffset() == startOffset && it->Key.GetPartNo() > partNo);
        --it;
        Y_ABORT_UNLESS(it->Key.GetOffset() < startOffset || (it->Key.GetOffset() == startOffset && it->Key.GetPartNo() <= partNo));
        ui32 cnt = 0;
        ui32 sz = 0;
        if (startOffset > it->Key.GetOffset() + it->Key.GetCount()) { //there is a gap
            ++it;
            if (it != DataKeysBody.end()) {
                cnt = it->Key.GetCount();
                sz = it->Size;
            }
        } else {
            Y_ABORT_UNLESS(it->Key.GetCount() >= (startOffset - it->Key.GetOffset()));
            cnt = it->Key.GetCount() - (startOffset - it->Key.GetOffset()); //don't count all elements from first blob
            sz = (cnt == it->Key.GetCount() ? it->Size : 0); //not readed client blobs can be of ~8Mb, so don't count this size at all
        }
        while (it != DataKeysBody.end()
               && (size < maxSize && count < maxCount || count == 0) //count== 0 grants that blob with offset from ReadFromTimestamp will be readed
               && (lastOffset == 0 || it->Key.GetOffset() < lastOffset)
        ) {
            size += sz;
            count += cnt;
            TRequestedBlob reqBlob(it->Key.GetOffset(), it->Key.GetPartNo(), it->Key.GetCount(),
                                   it->Key.GetInternalPartsCount(), it->Size, TString(), it->Key);
            blobs.push_back(reqBlob);

            blobKeyTokens->Append(it->BlobKeyToken);

            ++it;
            if (it == DataKeysBody.end())
                break;
            sz = it->Size;
            cnt = it->Key.GetCount();
        }
    }
    return blobs;
}

TVector<TClientBlob> TPartitionBlobEncoder::GetBlobsFromHead(const ui64 startOffset,
                                                             const ui16 partNo,
                                                             const ui32 maxCount,
                                                             const ui32 maxSize,
                                                             const ui64 readTimestampMs,
                                                             ui32& count,
                                                             ui32& size,
                                                             ui64& insideHeadOffset,
                                                             ui64 lastOffset) const
{
    TVector<TClientBlob> res;
    std::optional<ui64> firstAddedBlobOffset{};
    ui32 pos = 0;
    if (PositionInHead(startOffset, partNo)) {
        pos = Head.FindPos(startOffset, partNo);
        Y_ABORT_UNLESS(pos != Max<ui32>());
    }
    ui32 lastBlobSize = 0;
    for (; pos < Head.GetBatches().size(); ++pos) {

        TVector<TClientBlob> blobs;
        Head.GetBatch(pos).UnpackTo(&blobs);
        ui32 i = 0;
        ui64 offset = Head.GetBatch(pos).GetOffset();
        ui16 pno = Head.GetBatch(pos).GetPartNo();
        for (; i < blobs.size(); ++i) {

            ui64 curOffset = offset;

            Y_ABORT_UNLESS(pno == blobs[i].GetPartNo());
            bool skip = offset < startOffset || offset == startOffset &&
                blobs[i].GetPartNo() < partNo;
            if (0 < lastOffset && lastOffset <= offset) {
                break;
            }
            if (blobs[i].IsLastPart()) {
                ++offset;
                pno = 0;
            } else {
                ++pno;
            }

            if (skip) continue;

            if (blobs[i].IsLastPart()) {
                bool messageSkippingBehaviour = AppData()->PQConfig.GetTopicsAreFirstClassCitizen() &&
                        readTimestampMs > blobs[i].WriteTimestamp.MilliSeconds();
                ++count;
                if (messageSkippingBehaviour) { //do not count in limits; message will be skippend in proxy
                    --count;
                    size -= lastBlobSize;
                }
                lastBlobSize = 0;

                if (count > maxCount) {// blob is counted already
                    break;
                }
                if (size > maxSize) {
                    break;
                }
            }
            size += blobs[i].GetBlobSize();
            lastBlobSize += blobs[i].GetBlobSize();
            res.push_back(blobs[i]);

            if (!firstAddedBlobOffset)
                firstAddedBlobOffset = curOffset;

        }
        if (i < blobs.size()) // already got limit
            break;
    }
    insideHeadOffset = firstAddedBlobOffset.value_or(insideHeadOffset);
    return res;
}

ui64 TPartitionBlobEncoder::GetHeadGapSize() const
{
    return DataKeysBody.empty() ? 0 : (Head.Offset - (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount()));
}

ui64 TPartitionBlobEncoder::GetSizeLag(i64 offset) const
{
    ui64 sizeLag = 0;
    if (!DataKeysBody.empty() && PositionInBody(offset, 0)) { // there will be something in body
        auto it = std::upper_bound(DataKeysBody.begin(), DataKeysBody.end(), std::make_pair(offset, 0),
                [](const std::pair<ui64, ui16>& offsetAndPartNo, const TDataKey& p) { return offsetAndPartNo.first < p.Key.GetOffset() || offsetAndPartNo.first == p.Key.GetOffset() && offsetAndPartNo.second < p.Key.GetPartNo();});
        if (it != DataKeysBody.begin())
            --it; //point to blob with this offset
        Y_ABORT_UNLESS(it != DataKeysBody.end());
        sizeLag = it->Size + DataKeysBody.back().CumulativeSize - it->CumulativeSize;
        Y_ABORT_UNLESS(BodySize == DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size - DataKeysBody.front().CumulativeSize,
                       "BodySize=%" PRIu64
                       ", DataKeysBody.back.CumulativeSize=%" PRIu64 ", DataKeysBody.back.Size=%" PRIu64
                       ", DataKeysBody.front.CumulativeSize=%" PRIu64,
                       BodySize,
                       DataKeysBody.back().CumulativeSize, DataKeysBody.back().Size,
                       DataKeysBody.front().CumulativeSize);
    }
    for (const auto& b : HeadKeys) {
        if ((i64)b.Key.GetOffset() >= offset)
            sizeLag += b.Size;
    }
    return sizeLag;
}

bool TPartitionBlobEncoder::PositionInBody(ui64 offset, ui32 partNo) const
{
    return offset < Head.Offset || ((Head.Offset == offset) && (partNo < Head.PartNo));
}

bool TPartitionBlobEncoder::PositionInHead(ui64 offset, ui32 partNo) const
{
    return Head.Offset < offset || ((Head.Offset == offset) && (Head.PartNo < partNo));
}

bool TPartitionBlobEncoder::IsEmpty() const
{
    return DataKeysBody.empty() && HeadKeys.empty();
}

const TDataKey* TPartitionBlobEncoder::GetLastKey() const
{
    const TDataKey* lastKey = nullptr;
    if (!HeadKeys.empty()) {
        lastKey = &HeadKeys.back();
    } else if (!DataKeysBody.empty()) {
        lastKey = &DataKeysBody.back();
    }
    return lastKey;
}

TString TPartitionBlobEncoder::SerializeForKey(const TKey& key, ui32 size,
                                               ui64 endOffset,
                                               TInstant& writeTimestamp) const
{
    TString valueD;
    valueD.reserve(size);

    ui32 pp = Head.FindPos(key.GetOffset(), key.GetPartNo());
    if (pp < Max<ui32>() && key.GetOffset() < endOffset) { //this batch trully contains this offset
        Y_ABORT_UNLESS(pp < Head.GetBatches().size());
        Y_ABORT_UNLESS(Head.GetBatch(pp).GetOffset() == key.GetOffset());
        Y_ABORT_UNLESS(Head.GetBatch(pp).GetPartNo() == key.GetPartNo());

        for (; pp < Head.GetBatches().size(); ++pp) { //TODO - merge small batches here
            Y_ABORT_UNLESS(Head.GetBatch(pp).Packed);
            const auto& b = Head.GetBatch(pp);
            b.SerializeTo(valueD);
            writeTimestamp = std::max(writeTimestamp, b.GetEndWriteTimestamp());
        }
    }

    for (const auto& b : NewHead.GetBatches()) {
        Y_ABORT_UNLESS(b.Packed,
                       "key=%s",
                       key.ToString().data());
        b.SerializeTo(valueD);
        writeTimestamp = std::max(writeTimestamp, b.GetEndWriteTimestamp());
    }

    Y_ABORT_UNLESS(size >= valueD.size());

    if (size > valueD.size() && key.HasSuffix()) { //change to real size if real packed size is smaller
        Y_ABORT("Can't be here right now, only after merging of small batches");

        //for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
        //    if (it->KeysCount() > 0 ) {
        //        auto res2 = it->PopBack();
        //        Y_ABORT_UNLESS(res2 == res);
        //        res2.second = valueD.size();

        //        DataKeysHead[TotalLevels - 1].AddKey(res2.first, res2.second);

        //        res2 = Compact(res2.first, res2.second, headCleared);

        //        Y_ABORT_UNLESS(res2.first == key);
        //        Y_ABORT_UNLESS(res2.second == valueD.size());
        //        res = res2;
        //        break;
        //    }
        //}
    }

    Y_ABORT_UNLESS(size == valueD.size() || key.HasSuffix());

    TClientBlob::CheckBlob(key, valueD);

    return valueD;
}

TKey TPartitionBlobEncoder::KeyForWrite(TKeyPrefix::EType type,
                                        const TPartitionId& partitionId,
                                        bool needCompaction) const
{
    Y_ABORT_UNLESS(!ForFastWrite);
    if (needCompaction) {
        return TKey::ForBody(type, partitionId, NewHead.Offset, NewHead.PartNo, NewHead.GetCount(), NewHead.GetInternalPartsCount());
    }
    return TKey::ForHead(type, partitionId, NewHead.Offset, NewHead.PartNo, NewHead.GetCount(), NewHead.GetInternalPartsCount());
}

TKey TPartitionBlobEncoder::KeyForFastWrite(TKeyPrefix::EType type, const TPartitionId& partitionId) const
{
    Y_ABORT_UNLESS(ForFastWrite);
    return TKey::ForFastWrite(type, partitionId, NewHead.Offset, NewHead.PartNo, NewHead.GetCount(), NewHead.GetInternalPartsCount());
}

void TPartitionBlobEncoder::NewPartitionedBlob(const TPartitionId& partitionId,
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
                                       nextPartNo,
                                       ForFastWrite);
}

void TPartitionBlobEncoder::ClearPartitionedBlob(const TPartitionId& partitionId, ui32 maxBlobSize)
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

void TPartitionBlobEncoder::SyncHeadKeys()
{
    if (!CompactedKeys.empty()) {
        HeadKeys.clear();
    }
}

void TPartitionBlobEncoder::SyncNewHeadKey()
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

void TPartitionBlobEncoder::SyncDataKeysBody(TInstant now,
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
        //Y_ABORT_UNLESS(!key.HasSuffix(),
        //               "key=%s",
        //               key.ToString().data());

        BodySize += blobSize;

        ui64 lastOffset = DataKeysBody.empty() ? 0 : getNextOffset(DataKeysBody.back().Key);
        //if (!(lastOffset <= key.GetOffset())) {
        //    Dump();
        //}
        Y_ABORT_UNLESS(lastOffset <= key.GetOffset(),
                       "lastOffset=%" PRIu64 ", key=%s",
                       lastOffset, key.ToString().data());

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

void TPartitionBlobEncoder::SyncHeadFromNewHead()
{
    Head.PackedSize = 0;
    Head.Offset = NewHead.Offset;
    Head.PartNo = NewHead.PartNo; //no partNo at this point
    Head.ClearBatches();
}

void TPartitionBlobEncoder::SyncHead(ui64& startOffset, ui64& endOffset)
{
    Y_ABORT_UNLESS(!ForFastWrite);
    //append Head with newHead
    while (!NewHead.GetBatches().empty()) {
        Head.AddBatch(NewHead.ExtractFirstBatch());
    }
    Head.PackedSize += NewHead.PackedSize;

    if (Head.PackedSize > 0 && DataKeysBody.empty()) {
        startOffset = Head.Offset + (Head.PartNo > 0 ? 1 : 0);
    }

    endOffset = Head.GetNextOffset();
}

void TPartitionBlobEncoder::SyncHeadFastWrite(ui64& startOffset, ui64& endOffset)
{
    Y_ABORT_UNLESS(ForFastWrite);
    Y_ABORT_UNLESS(Head.PackedSize == 0,
                   "Head.PackedSize=%" PRIu32, Head.PackedSize);

    // We calculate the initial offset if this is the first write operation.
    if (NewHead.PackedSize > 0 && DataKeysBody.empty()) {
        Y_ABORT_UNLESS(Head.Offset == NewHead.Offset);
        Y_ABORT_UNLESS(Head.PartNo == NewHead.PartNo);

        startOffset = NewHead.Offset + (NewHead.PartNo > 0 ? 1 : 0);
    }

    // In the FastWrite zone, everything is stored in the body. That's why we need to move the keys out of my head.
    for (auto& k : HeadKeys) {
        BodySize += k.Size;
        k.CumulativeSize = DataKeysBody.empty() ? 0 : (DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size);
        DataKeysBody.push_back(std::move(k));
    }
    HeadKeys.clear();

    // Here is the Head.Packed Size != 0. Therefore, the keys must be in the body.
    Y_ABORT_UNLESS(!DataKeysBody.empty());

    endOffset = NewHead.GetNextOffset();

    // In the FastWrite zone, the head should be empty after the write operation.
    Head.Clear();
    Head.Offset = endOffset;

    // There's nothing in my head. Therefore, the packaging levels also need to be cleaned.
    for (ui32 i = 0; i < DataKeysHead.size(); ++i) {
        DataKeysHead[i].Clear();
    }
}

void TPartitionBlobEncoder::ResetNewHead(ui64 endOffset)
{
    NewHead.Clear();
    NewHead.Offset = endOffset;
}

bool TPartitionBlobEncoder::IsNothingWritten() const
{
    return CompactedKeys.empty() && (NewHead.PackedSize == 0);
}

bool TPartitionBlobEncoder::IsLastBatchPacked() const
{
    return NewHead.GetBatches().empty() || NewHead.GetLastBatch().Packed;
}

void TPartitionBlobEncoder::PackLastBatch()
{
    NewHead.MutableLastBatch().Pack();
    NewHead.PackedSize += NewHead.GetLastBatch().GetPackedSize(); //add real packed size for this blob

    NewHead.PackedSize -= GetMaxHeaderSize(); //instead of upper bound
    NewHead.PackedSize -= NewHead.GetLastBatch().GetUnpackedSize();
}

std::pair<TKey, ui32> TPartitionBlobEncoder::Compact(const TKey& key, bool headCleared)
{
    const ui32 size = NewHead.PackedSize;
    std::pair<TKey, ui32> res(key, size);
    ui32 x = headCleared ? 0 : Head.PackedSize;
    Y_ABORT_UNLESS(std::accumulate(DataKeysHead.begin(), DataKeysHead.end(), 0u, [](ui32 sum, const TKeyLevel& level){return sum + level.Sum();}) == NewHead.PackedSize + x);
    for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
        auto jt = it; ++jt;
        if (it->NeedCompaction()) {
            res = it->Compact();
            if (jt != DataKeysHead.rend()) {
                jt->AddKey(res.first, res.second);
            }
        } else {
            Y_ABORT_UNLESS(jt == DataKeysHead.rend() || !jt->NeedCompaction()); //compact must start from last level, not internal
        }
        Y_ABORT_UNLESS(!it->NeedCompaction());
    }
    Y_ABORT_UNLESS(res.second >= size);
    Y_ABORT_UNLESS(res.first.GetOffset() < key.GetOffset() || res.first.GetOffset() == key.GetOffset() && res.first.GetPartNo() <= key.GetPartNo());
    return res;
}

//void TPartitionBlobEncoder::Dump() const
//{
//    auto dumpCompactedKeys = [this](const std::deque<std::pair<TKey, ui32>>& keys, const char* prefix) {
//        Y_UNUSED(this);
//        Y_UNUSED(prefix);
//        for (size_t i = 0; i < keys.size(); ++i) {
//            DBGTRACE_LOG(prefix << "[" << i << "]=" << keys[i].first.ToString() << " (" << keys[i].second << ")");
//        }
//    };
//    auto dumpKeys = [this](const std::deque<TDataKey>& keys, const char* prefix) {
//        Y_UNUSED(this);
//        Y_UNUSED(prefix);
//        if (keys.size() > 10) {
//            auto dumpSubkeys = [this](const std::deque<TDataKey>& keys, size_t begin, size_t end, const char* prefix) {
//                Y_UNUSED(this);
//                Y_UNUSED(keys);
//                Y_UNUSED(prefix);
//                for (size_t i = begin; i < end; ++i) {
//                    DBGTRACE_LOG(prefix << "[" << i << "]=" << keys[i].Key.ToString() <<
//                                 ", Size=" << keys[i].Size << ", CumulativeSize=" << keys[i].CumulativeSize);
//                }
//            };
//            dumpSubkeys(keys, 0, 3, prefix);
//            DBGTRACE_LOG("...");
//            dumpSubkeys(keys, keys.size() - 3, keys.size(), prefix);
//            return;
//        }
//        for (size_t i = 0; i < keys.size(); ++i) {
//            DBGTRACE_LOG(prefix << "[" << i << "]=" << keys[i].Key.ToString() <<
//                         ", Size=" << keys[i].Size << ", CumulativeSize=" << keys[i].CumulativeSize);
//        }
//    };
//    auto dumpHead = [this](const THead& head, const char* prefix) {
//        Y_UNUSED(this);
//        Y_UNUSED(head);
//        Y_UNUSED(prefix);
//        DBGTRACE_LOG(prefix <<
//                     ": Offset=" << head.Offset << ", PartNo=" << head.PartNo <<
//                     ", PackedSize=" << head.PackedSize <<
//                     ", Batches.size=" << head.GetBatches().size());
//    };
//    auto dumpDataKeysHead = [this](const TVector<TKeyLevel>& levels, const char* prefix) {
//        Y_UNUSED(this);
//        Y_UNUSED(prefix);
//        for (size_t i = 0; i < levels.size(); ++i) {
//            const auto& level = levels[i];
//            DBGTRACE_LOG(prefix << "[" << i << "] " << level.Sum() << " / " << level.Border());
//            for (ui32 j = 0; j < level.KeysCount(); ++j) {
//                DBGTRACE_LOG("    [" << j << "] " << level.GetKey(j).ToString() << " (" << level.GetSize(j) << ")");
//            }
//        }
//    };
//
//    DBGTRACE_LOG("StartOffset=" << StartOffset << ", EndOffset=" << EndOffset);
//    dumpCompactedKeys(CompactedKeys, "CompactedKeys");
//    DBGTRACE_LOG("BodySize=" << BodySize);
//    dumpKeys(DataKeysBody, "Body");
//    dumpKeys(HeadKeys, "Head");
//    dumpHead(Head, "Head");
//    dumpDataKeysHead(DataKeysHead, "Levels");
//    dumpHead(NewHead, "NewHead");
//    DBGTRACE_LOG("NewHeadKey=" << NewHeadKey.Key.ToString() << " (" << NewHeadKey.Size << ")");
//}

}
