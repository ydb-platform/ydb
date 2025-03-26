#include "lru.h"

namespace NLogin {

TLruCache::TLruCache(std::size_t capacity)
    : Capacity(capacity)
{
    Index.reserve(Capacity);
}

TLruCache::TIterator TLruCache::Find(const TKey& key) {
    const auto indexIt = Index.find(key);
    if (indexIt == Index.end()) {
        return Data.end();
    }

    Promote(indexIt->second);
    return indexIt->second;
}

std::pair<TLruCache::TIterator, bool> TLruCache::Insert(const TKey& key, bool value) {
    const auto indexIt = Index.find(key);
    if (indexIt != Index.end()) {
        Promote(indexIt->second);
        return std::make_pair(indexIt->second, false);
    }

    if (Capacity == 0) {
        return std::make_pair(Data.end(), false);
    }

    while (IsOverflow()) {
        Evict();
    }

    Data.push_front(std::make_pair(key, value));
    Index.emplace(key, Data.begin());
    return std::make_pair(Data.begin(), true);
}

std::size_t TLruCache::Size() const {
    return Index.size();
}

TLruCache::TIterator TLruCache::End() {
    return Data.end();
}

void TLruCache::Clear() {
    Data.clear();
    Index.clear();
}

bool TLruCache::IsOverflow() const {
    return Index.size() == Capacity;
}

void TLruCache::Promote(const TIterator it) {
    Data.splice(Data.begin(), Data, it);
}

void TLruCache::Evict() {
    const auto item = Data.back();
    Data.pop_back();
    Index.erase(item.first);
}

void TLruCache::Resize(size_t capacity) {
    Capacity = capacity;
    while (Index.size() > Capacity) {
        Evict();
    }
}

} // NLogin
