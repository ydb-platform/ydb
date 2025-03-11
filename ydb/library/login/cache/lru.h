#pragma once

#include <list>
#include <unordered_map>

#include <util/generic/string.h>
#include <util/str_stl.h>

namespace NLogin {

class TLruCache {
public:
    using TKey = std::pair<const TString, const TString>;
    using TItem = std::pair<const TKey, bool>;
    using TDataContainer = std::list<TItem>;
    using TIterator = TDataContainer::iterator;
    using TIndexContainer = std::unordered_map<TKey, const TIterator, THash<TKey>>;

    TLruCache(std::size_t capacity);

    TIterator Find(const TKey& key);
    std::pair<TIterator, bool> Insert(const TKey& key, bool value);
    std::size_t Size() const;
    TIterator End();
    void Clear();

private:
    bool IsOverflow() const;
    void Promote(const TIterator it);

private:
    std::size_t Capacity = 0;
    TDataContainer Data;
    TIndexContainer Index;
};

} // NLogin
