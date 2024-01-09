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

const TLogCache::TCacheRecord* TLogCache::Find(ui64 offset) {
    auto indexIt = Index.find(offset);
    if (indexIt == Index.end()) {
        return nullptr;
    }

    List.PushFront(&indexIt->second);
    return &indexIt->second.Value;
}

const TLogCache::TCacheRecord* TLogCache::FindWithoutPromote(ui64 offset) const {
    auto indexIt = Index.find(offset);
    if (indexIt == Index.end()) {
        return nullptr;
    }

    return &indexIt->second.Value;
}

bool TLogCache::Pop() {
    if (Index.empty())
        return false;

    TItem* item = List.PopBack();
    Index.erase(item->Value.Offset);
    return true;
}

bool TLogCache::Insert(TCacheRecord&& value) {
    auto [it, inserted] = Index.try_emplace(value.Offset, std::move(value));
    List.PushFront(&it->second);
    return inserted;
}

size_t TLogCache::Erase(ui64 offset) {
    return Index.erase(offset);
}

size_t TLogCache::EraseRange(ui64 begin, ui64 end) {
    Y_DEBUG_ABORT_UNLESS(begin <= end);
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
