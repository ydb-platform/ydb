#include "blobstorage_pdisk_log_cache.h"

namespace NKikimr {
namespace NPDisk {

TLogCache::TCacheRecord::TCacheRecord(ui64 offset, TRcBuf data, TVector<ui64> badOffsets)
    : Offset(offset)
    , Data(std::move(data))
    , BadOffsets(std::move(badOffsets))
{}

TLogCache::TCacheRecord::TCacheRecord(TCacheRecord&& other)
    : Offset(other.Offset)
    , Data(std::move(other.Data))
    , BadOffsets(std::move(other.BadOffsets))
{}

TLogCache::TItem::TItem(TItem&& other) 
    : Value(std::move(other.Value))
{}

TLogCache::TItem::TItem(TCacheRecord&& value)
    : Value(std::move(value))
{}

size_t TLogCache::Size() const {
    return Index.size();
}

template <typename C>
typename C::iterator
FindKeyLess(C& c, const typename C::key_type& key) {
    auto iter = c.lower_bound(key);
    
    if (iter == c.begin()) {
        return c.end();
    }

    return --iter;
}

template <typename C>
typename C::iterator
FindKeyLessEqual(C& c, const typename C::key_type& key) {
    auto iter = c.upper_bound(key);
    
    if (iter == c.begin()) {
        return c.end();
    }

    return --iter;
}

bool TLogCache::Find(ui64 offset, ui32 size, char* buffer, TBadOffsetsHandler func) {
    return Find(offset, size, buffer, func, true);
}

bool TLogCache::FindWithoutPromote(ui64 offset, ui32 size, char* buffer, TBadOffsetsHandler func) {
    return Find(offset, size, buffer, func, false);
}

bool TLogCache::Find(ui64 offset, ui32 size, char* buffer, std::function<void(const std::vector<ui64>&)> func, bool promote) {
    TVector<TItem*> res;

    auto indexIt = FindKeyLessEqual(Index, offset);

    if (indexIt == Index.end()) {
        return false;
    }

    ui64 cur = offset;
    ui64 end = offset + size;

    while (indexIt != Index.end() && cur < end) {
        ui64 recStart = indexIt->first;
        ui64 recEnd = recStart + indexIt->second.Value.Data.Size();

        if (cur >= recStart && cur < recEnd) {
            res.push_back(&indexIt->second);
        } else {
            return false;
        }

        cur = recEnd;

        indexIt++;
    }

    if (cur < end) {
        return false;
    }

    for (auto item : res) {
        auto cacheRecord = &item->Value;

        ui64 recStart = cacheRecord->Offset;
        ui64 recEnd = recStart + cacheRecord->Data.Size();

         // Determine the buffer's chunk start and end absolute offsets.
        ui64 chunkStartOffset = std::max(recStart, offset);
        ui64 chunkEndOffset = std::min(recEnd, offset + size);
        ui64 chunkSize = chunkEndOffset - chunkStartOffset;

        // Calculate the chunk's position within the buffer to start copying.
        ui64 chunkOffset = chunkStartOffset - recStart;

        // Copy the chunk data to the buffer.
        std::memcpy(buffer + (chunkStartOffset - offset), cacheRecord->Data.Data() + chunkOffset, chunkSize);

        // Notify callee of bad offsets.
        func(cacheRecord->BadOffsets);

        if (promote) {
            List.PushFront(item);
        }
    }

    return true;
}

bool TLogCache::Pop() {
    if (Index.empty())
        return false;

    TItem* item = List.PopBack();
    Index.erase(item->Value.Offset);
    return true;
}

std::pair<i64, i64> TLogCache::PrepareInsertion(ui64 start, ui32 size) {
    ui64 end = start + size;
    ui32 leftPadding = 0;
    ui32 rightPadding = 0;

    // Check if there is a block that overlaps with the new insertion's start.
    auto it1 = FindKeyLessEqual(Index, start);
    if (it1 != Index.end()) {
        ui64 maybeStart = it1->first;
        ui64 maybeEnd = maybeStart + it1->second.Value.Data.Size();

        if (start < maybeEnd) {
            if (end <= maybeEnd) {
                return {-1, -1}; // There is an overlapping block; return {-1, -1} to indicate it.
            }
            leftPadding = maybeEnd - start;
        }
    }

    // Check if there is a block that overlaps with the new insertion's end.
    auto it2 = FindKeyLess(Index, end);
    if (it2 != Index.end()) {
        ui64 maybeStart = it2->first;
        ui64 maybeEnd = maybeStart + it2->second.Value.Data.Size();

        if (end < maybeEnd) {
            rightPadding = end - maybeStart;
        }
    }

    // Remove any blocks that are completely covered by the new insertion.
    ui64 offsetStart = start + leftPadding;
    ui64 offsetEnd = start + (size - rightPadding);

    auto it = Index.upper_bound(offsetStart);
    while (it != Index.end()) {
        ui64 blockEnd = it->first + it->second.Value.Data.Size();
        if (blockEnd < offsetEnd) {
            it = Index.erase(it);
        } else {
            break;
        }
    }

    return {leftPadding, rightPadding};
}

bool TLogCache::Insert(const char* dataPtr, ui64 offset, ui32 size, const TVector<ui64>& badOffsets) {
    auto [leftPadding, rightPadding] = PrepareInsertion(offset, size);

    if (leftPadding == -1 && rightPadding == -1) {
        return false;
    }

    auto dataStart = dataPtr + leftPadding;
    auto dataEnd = dataPtr + (size - rightPadding);

    Y_VERIFY_DEBUG(dataStart < dataEnd);

    auto [it, inserted] = Index.try_emplace(offset + leftPadding, std::move(TLogCache::TCacheRecord(
            offset + leftPadding,
            TRcBuf(TString(dataStart, dataEnd)),
            badOffsets)
    ));

    Y_VERIFY_DEBUG(inserted);

    List.PushFront(&it->second); 

    return true;
}

size_t TLogCache::EraseRange(ui64 begin, ui64 end) {
    Y_VERIFY_DEBUG(begin <= end);
    auto beginIt = Index.lower_bound(begin);
    auto endIt = Index.lower_bound(end);
    size_t dist = std::distance(beginIt, endIt);
    Index.erase(beginIt, endIt);
    return dist;
}

void TLogCache::Clear() {
    Index.clear();
}

} // NPDisk
} // NKikimr
