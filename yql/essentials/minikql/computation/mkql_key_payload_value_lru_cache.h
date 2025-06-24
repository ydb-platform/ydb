#pragma once

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <util/generic/string.h>
#include <unordered_map>
#include <list>
#include <chrono>

namespace NKikimr::NMiniKQL {

//Design notes:
//  Commodity LRUCache with HashMap and DoubleLinkedList. Key(that is TUnboxedValue) is stored in both HashMap and List
//  Lazy(postponed) TTL implementation. An entry is not deleted immediately when its TTL is expired, the deletion is postponed until:
//   - when it is accessed via Get()
//   - when it is not accessed for a long period of time and becomes the LRU. Then it is garbage collected in Tick()
//  Not thread safe
//  Never requests system time, expects monotonically increased time points in methods argument
class TUnboxedKeyValueLruCacheWithTtl {
    struct TEntry {
        TEntry(NUdf::TUnboxedValue key, NUdf::TUnboxedValue value, std::chrono::time_point<std::chrono::steady_clock> expiration)
            : Key(std::move(key))
            , Value(std::move(value))
            , Expiration(std::move(expiration))
        {}
        NUdf::TUnboxedValue Key;
        NUdf::TUnboxedValue Value;
        std::chrono::time_point<std::chrono::steady_clock> Expiration;
    };
    using TUsageList = std::list<TEntry>;

public:
    TUnboxedKeyValueLruCacheWithTtl(size_t maxSize, const NKikimr::NMiniKQL::TType* keyType)
        : MaxSize_(maxSize)
        , KeyTypeHelper_(keyType)
        , Map_(
            1000,
            KeyTypeHelper_.GetValueHash(),
            KeyTypeHelper_.GetValueEqual()
        )
    {
        Y_ABORT_UNLESS(MaxSize_ > 0);
    }
    TUnboxedKeyValueLruCacheWithTtl(const TUnboxedKeyValueLruCacheWithTtl&) = delete; //to prevent unintentional copy of a large object

    void Update(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& value, std::chrono::time_point<std::chrono::steady_clock>&& expiration) {
        if (auto it = Map_.find(key); it != Map_.end()) {
            Touch(it->second);
            auto& entry = *it->second;
            entry.Value = std::move(value);
            entry.Expiration = std::move(expiration);
        } else {
            if (Map_.size() == MaxSize_) {
                RemoveLeastRecentlyUsedEntry();
            }
            UsageList_.emplace_back(key, std::move(value), std::move(expiration));
            Map_.emplace_hint(it, std::move(key), --UsageList_.end());
        }
    }

    std::optional<NUdf::TUnboxedValue> Get(const NUdf::TUnboxedValue key, const std::chrono::time_point<std::chrono::steady_clock>& now) {
        if (auto it = Map_.find(key); it != Map_.end()) {
            if (now < it->second->Expiration) {
                Touch(it->second);
                return it->second->Value;
            } else {
                UsageList_.erase(it->second);
                Map_.erase(it);
                return std::nullopt;
            }
        }
        return std::nullopt;
    }

    // Perform garbage collection, single step, O(1) time.
    // Must be called periodically
    bool Tick(const std::chrono::time_point<std::chrono::steady_clock>& now) {
        if (UsageList_.empty()) {
            return false;
        }
        if (now < UsageList_.front().Expiration) {
            return false;
        }
        RemoveLeastRecentlyUsedEntry();
        return true;
    }

    // Perform garbage collection, O(1) amortized, but O(n) one-time
    void Prune(const std::chrono::time_point<std::chrono::steady_clock>& now) {
        while (Tick(now)) {
        }
    }

    size_t Size() const {
        Y_ABORT_UNLESS(Map_.size() == UsageList_.size());
        return Map_.size();
    }
private:
    struct TKeyTypeHelpers {
            TKeyTypes KeyTypes;
            bool IsTuple;
            NUdf::IHash::TPtr Hash;
            NUdf::IEquate::TPtr Equate;
    };

    void Touch(TUsageList::iterator it) {
        UsageList_.splice(UsageList_.end(), UsageList_, it); //move accessed element to the end of Usage list
    }
    void RemoveLeastRecentlyUsedEntry() {
        Map_.erase(UsageList_.front().Key);
        UsageList_.pop_front();
    }
private:
    const size_t MaxSize_;
    TUsageList UsageList_;
    const TKeyTypeContanerHelper<true, true, false> KeyTypeHelper_;
    std::unordered_map<
        NUdf::TUnboxedValue,
        TUsageList::iterator,
        TValueHasher,
        TValueEqual,
        NKikimr::NMiniKQL::TMKQLAllocator<std::pair<const NUdf::TUnboxedValue, TUsageList::iterator>>
    > Map_;

};

} //namespace NKikimr::NMiniKQL
