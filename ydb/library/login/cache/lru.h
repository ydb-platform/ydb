#pragma once

#include <list>
#include <unordered_map>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/str_stl.h>

namespace NLogin {

class TLruCache {
public:
    struct TKey {
        TString User;
        TString Password;
        TString Hash;

        bool operator == (const TKey& other) const {
            return ((this->User == other.User) && (this->Password == other.Password) && (this->Hash == other.Hash));
        }
    };

    struct TKeyHash {
        size_t operator() (const TKey& key) const {
            return THash<TString>()(TStringBuilder() << key.User << key.Password << key.Hash);
        }
    };

    using TItem = std::pair<const TKey, bool>;
    using TDataContainer = std::list<TItem>;
    using TIterator = TDataContainer::iterator;
    using TIndexContainer = std::unordered_map<TKey, const TIterator, TKeyHash>;

    TLruCache(std::size_t capacity);

    TIterator Find(const TKey& key);
    std::pair<TIterator, bool> Insert(const TKey& key, bool value);
    std::size_t Size() const;
    TIterator End();
    void Clear();
    void Resize(size_t capacity);

private:
    bool IsOverflow() const;
    void Promote(const TIterator it);
    void Evict();

private:
    std::size_t Capacity = 0;
    TDataContainer Data;
    TIndexContainer Index;
};

} // NLogin
