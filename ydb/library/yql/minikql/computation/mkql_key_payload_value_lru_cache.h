#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
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
        : MaxSize(maxSize)
        , KeyTypeHelper(keyType)
        , Map(
            1000,
            KeyTypeHelper.GetValueHash(),
            KeyTypeHelper.GetValueEqual()
        )
    {
        Y_ABORT_UNLESS(MaxSize > 0);
    }
    TUnboxedKeyValueLruCacheWithTtl(const TUnboxedKeyValueLruCacheWithTtl&) = delete; //to prevent unintentional copy of a large object

    void Update(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& value, std::chrono::time_point<std::chrono::steady_clock>&& expiration) {
        if (auto it = Map.find(key); it != Map.end()) {
            Touch(it->second);
            auto& entry = *it->second;
            entry.Value = std::move(value);
            entry.Expiration = std::move(expiration);
        } else {
            if (Map.size() == MaxSize) {
                RemoveLeastRecentlyUsedEntry();
            }
            UsageList.emplace_back(key, std::move(value), std::move(expiration));
            Map.emplace_hint(it, std::move(key), --UsageList.end());
        }
    }

    std::optional<NUdf::TUnboxedValue> Get(const NUdf::TUnboxedValue key, const std::chrono::time_point<std::chrono::steady_clock>& now) {
        if (auto it = Map.find(key); it != Map.end()) {
            if (now < it->second->Expiration) {
                Touch(it->second);
                return it->second->Value;
            } else {
                UsageList.erase(it->second);
                Map.erase(it);
                return std::nullopt;
            }
        }
        return std::nullopt;
    }

    // Perform garbage collection.
    // Must be called periodically
    void Tick(const std::chrono::time_point<std::chrono::steady_clock>& now) {
        if (UsageList.empty()) {
            return;
        }
        if (now < UsageList.front().Expiration) {
            return;
        }
        RemoveLeastRecentlyUsedEntry();
    }

    size_t Size() const {
        Y_ABORT_UNLESS(Map.size() == UsageList.size());
        return Map.size();
    }
private:
    struct TKeyTypeHelpers {
            TKeyTypes KeyTypes;
            bool IsTuple;
            NUdf::IHash::TPtr Hash;
            NUdf::IEquate::TPtr Equate;
    };

    void Touch(TUsageList::iterator it) {
        UsageList.splice(UsageList.end(), UsageList, it); //move accessed element to the end of Usage list
    }
    void RemoveLeastRecentlyUsedEntry() {
        Map.erase(UsageList.front().Key);
        UsageList.pop_front();
    }
private:
    const size_t MaxSize;
    TUsageList UsageList;
    const TKeyTypeContanerHelper<true, true, false> KeyTypeHelper;
    std::unordered_map<
        NUdf::TUnboxedValue, 
        TUsageList::iterator,
        TValueHasher,
        TValueEqual,
        NKikimr::NMiniKQL::TMKQLAllocator<std::pair<const NUdf::TUnboxedValue, TUsageList::iterator>>
    > Map;

};

} //namespace NKikimr::NMiniKQL
