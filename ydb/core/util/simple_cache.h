#pragma once

#include <unordered_map>
#include <list>

namespace NKikimr {

template <typename TKey, typename TValue>
class TSimpleCache {
public:
    size_t MaxSize = 1024;

    struct TItem;

    using TMap = std::unordered_map<TKey, TItem>;
    using TMapIterator = typename TMap::iterator;
    using TList = typename std::list<TMapIterator>;
    using TListIterator = typename TList::iterator;

    struct TItem {
        TValue Value;
        TListIterator ListIterator;
    };

    TValue* FindPtr(TKey key) {
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            Touch(it);
            return &it->second.Value;
        } else {
            return nullptr;
        }
    }

    TValue& Update(TKey key, TValue value = {}) {
        auto it = Map_.find(key); // we don't use blind emplace to avoid coping value
        if (it == Map_.end()) {
            it = Map_.emplace(key, TItem{std::move(value), List_.end()}).first;
            List_.emplace_back(it);
            it->second.ListIterator = std::prev(List_.end());
            Shrink();
        } else {
            it->second.Value = std::move(value);
            Touch(it);
        }
        return it->second.Value;
    }

    void Erase(TKey key) {
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            List_.remove(it); // log(N)
            Map_.erase(it);
        }
    }

protected:
    void Touch(TMapIterator it) {
        List_.splice(List_.end(), List_, it->second.ListIterator);
        it->second.ListIterator = std::prev(List_.end());
    }

    void Shrink() {
        if (List_.size() > MaxSize) {
            Map_.erase(List_.front());
            List_.pop_front();
        }
    }

    TMap Map_;
    TList List_;
};

template <typename TKey, typename TValue>
class TNotSoSimpleCache {
public:
    size_t MaxSize = 1024;

    struct TItem;

    using TMap = std::unordered_map<TKey, TItem>;
    using TMapIterator = typename TMap::iterator;
    using TList = typename std::list<TMapIterator>;
    using TListIterator = typename TList::iterator;

    struct TItem {
        TValue Value;
        TListIterator ListIterator;
    };

    TValue* FindPtr(TKey key) {
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            Touch(it);
            return &it->second.Value;
        } else {
            return nullptr;
        }
    }

    TValue& Update(TKey key, TValue value = {}) {
        auto it = Map_.find(key); // we don't use blind emplace to avoid coping value
        if (it == Map_.end()) {
            it = Map_.emplace(key, TItem{std::move(value), List_.end()}).first;
            List_.emplace_back(it);
            it->second.ListIterator = std::prev(List_.end());
            Shrink();
        } else {
            it->second.Value = std::move(value);
            Touch(it);
        }
        return it->second.Value;
    }

    void Erase(TKey key) {
        auto it = Map_.find(key);
        if (it != Map_.end()) {
            List_.remove(it); // log(N)
            Map_.erase(it);
        }
    }

protected:
    void Touch(TMapIterator it) {
        List_.splice(List_.end(), List_, it->second.ListIterator);
        it->second.ListIterator = std::prev(List_.end());
    }

    template<typename TVal, typename Enable = void>
    struct TReleaseChecker {
        bool operator ()(TVal&) const {
            return true;
        }
    };

    template<typename TVal>
    struct TReleaseChecker<TVal, typename std::enable_if_t<std::is_invocable_v<decltype(&TVal::IsSafeToRelease), TVal&>>> {
        bool operator ()(TVal& val) const {
            return val.IsSafeToRelease();
        }
    };

    static bool IsSafeToRelease(TValue& val) {
        return TReleaseChecker<TValue>()(val);
    }

    void Shrink() {
        size_t maxDepth = std::min<size_t>(MaxSize + 1, 10);
        auto itList = List_.begin();
        while (List_.size() > MaxSize && itList != List_.end()) {
            if (IsSafeToRelease((*itList)->second.Value)) {
                Map_.erase(*itList);
                itList = List_.erase(itList);
            } else {
                ++itList;
                if (--maxDepth == 0) {
                    break;
                }
            }
        }
    }

    TMap Map_;
    TList List_;
};

} // NKikimr
