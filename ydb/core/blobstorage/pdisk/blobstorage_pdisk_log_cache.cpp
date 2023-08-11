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
    TVector<TCacheRecord*> res;

    auto indexIt = FindKeyLessEqual(Index, offset);

    if (indexIt == Index.end()) {
        return false;
    }

    ui64 cur = offset;
    ui64 end = offset + size;

    while (indexIt != Index.end() && cur < end) {
        ui64 recStart = indexIt->first;
        ui64 recEnd = recStart + indexIt->second.Data.Size();

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

    for (auto cacheRecord : res) {
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
    }

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
        ui64 maybeEnd = maybeStart + it1->second.Data.Size();

        if (start < maybeEnd) {
            if (end <= maybeEnd) {
                return {-1, -1}; // There is an overlapping block; return {-1, -1} to indicate it.
            }
            leftPadding = maybeEnd - start;
        }
    }

    ui64 offsetStart = start + leftPadding;

    // Check if there is a block that overlaps with the new insertion's end.
    auto it2 = FindKeyLess(Index, end);
    if (it2 != Index.end()) {
        ui64 dataSize = it2->second.Data.Size();

        ui64 maybeStart = it2->first;
        ui64 maybeEnd = maybeStart + dataSize;

        if (offsetStart == maybeStart) {
            // There is an overlapping block; return {-1, -1} to indicate it.
            if (end <= maybeEnd) {
                return {-1, -1};
            }
            
            leftPadding += dataSize;
        }

        if (end < maybeEnd) {
            rightPadding = end - maybeStart;
        }
    }
    
    ui64 offsetEnd = start + (size - rightPadding);

    // Remove any blocks that are completely covered by the new insertion.
    auto it = Index.upper_bound(offsetStart);
    while (it != Index.end()) {
        ui64 blockEnd = it->first + it->second.Data.Size();
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
