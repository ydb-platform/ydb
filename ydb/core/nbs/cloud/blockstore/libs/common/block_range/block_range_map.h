#pragma once

#include "public.h"

#include "block_range.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator_adapter.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator_pool.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/stream/str.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// TBlockRangeMap is a class that manages a collection of block ranges and it
// key (ui64) with efficient overlap checking capabilities. It's designed to
// store and query block ranges and it key, particularly useful for determining
// if a given range overlaps with any of the stored ranges.
template <
    typename TKey,
    typename TValue,
    typename TRange = TBlockRange64,
    bool UseArenaAllocator = false>
class TBlockRangeMap
{
public:
    using TBlockRange = TRange;

    struct TFindItem
    {
        const TKey Key;
        const TBlockRange Range;
        TValue& Value;
    };

    struct TItem
    {
        TKey Key;
        TBlockRange Range;
        TValue Value;
    };

    enum class EEnumerateContinuation
    {
        Continue,
        Stop,
    };
    using TEnumerateFunc =
        std::function<EEnumerateContinuation(TFindItem& item)>;
    using TConstEnumerateFunc =
        std::function<EEnumerateContinuation(const TFindItem& item)>;

private:
    friend class TBlockRangeMapAccessor;

    struct TSearchKey
    {
        TKey Key;
        TBlockRange Range;
    };

    struct TItemKeyLess
    {
        using is_transparent = void;

        template <typename TLhs, typename TRhs>
        bool operator()(const TLhs& lhs, const TRhs& rhs) const
        {
            auto makeTie = [](const auto& item)
            {
                return std::tie(item.Range.End, item.Range.Start, item.Key);
            };
            return makeTie(lhs) < makeTie(rhs);
        }
    };

    using TRanges = std::conditional_t<
        UseArenaAllocator,
        TSet<TItem, TItemKeyLess, TArenaPoolAdapter<TItem>>,
        TSet<TItem, TItemKeyLess>>;
    using TRangeIt = decltype(TRanges().begin());
    using TRangeByKey = std::conditional_t<
        UseArenaAllocator,
        // When used arena allocator
        std::unordered_map<
            TKey,
            TRangeIt,
            THash<TKey>,
            TEqualTo<TKey>,
            TArenaPoolAdapter<std::pair<const TKey, TRangeIt>>>,
        // When used std::allocator
        THashMap<TKey, TRangeIt>>;

    static TFindItem MakeFindItem(TRangeIt it)
    {
        auto& item = const_cast<TItem&>(*it);
        return {.Key = item.Key, .Range = item.Range, .Value = item.Value};
    }

    template <typename TFunc>
    void EnumerateOverlappingImpl(TBlockRange other, TFunc f) const
    {
        // 1. Find the range x which: x.end >= other.start in the list sorted
        //    by end of range + length + key.
        // 2. Move through the list of ranges. Check overlapping x with other.
        // 3. when x.begin >= other.end + MaxLength stop iterating.

        auto left = TSearchKey{
            .Key = {},
            .Range = TBlockRange::MakeClosedInterval(0, other.Start)};
        const ui64 safeRight = (Max<ui64>() - MaxLength) > other.End
                                   ? other.End + MaxLength
                                   : Max<ui64>();
        for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
            if (it->Range.Overlaps(other)) {
                auto findItem = MakeFindItem(it);
                if (f(findItem) == EEnumerateContinuation::Stop) {
                    break;
                }
            }
            if (safeRight <= it->Range.Start) {
                break;
            }
        }
    }

    template <typename TFunc>
    void EnumerateImpl(TFunc f) const
    {
        for (auto it = Ranges.begin(); it != Ranges.end(); ++it) {
            auto findItem = MakeFindItem(it);
            if (f(findItem) == EEnumerateContinuation::Stop) {
                break;
            }
        }
    }

    ui64 MaxLength = 0;
    TRanges Ranges;
    TRangeByKey RangeByKey;

public:
    TBlockRangeMap() = default;

    explicit TBlockRangeMap(TArenaAllocatorPool* pool)
        : Ranges(typename TRanges::allocator_type(pool))
        , RangeByKey(typename TRangeByKey::allocator_type(pool))
    {}

    static TKey GetKeyByValue(const TValue& value)
    {
        return GetItemByValue(value).Key;
    }

    static TRange GetRangeByValue(const TValue& value)
    {
        return GetItemByValue(value).Range;
    }

    static TItem& GetItemByValue(const TValue& value)
    {
        constexpr size_t valueOffset = offsetof(TItem, Value);
        void* valueAddress = const_cast<TValue*>(&value);
        TItem* item = reinterpret_cast<TItem*>(
            static_cast<char*>(valueAddress) - valueOffset);
        return *item;
    }

    // Adds a block range to the collection. Returns false if the key already
    // exists in the collection.
    bool AddRange(TKey key, TBlockRange range, TValue value = {})
    {
        if (RangeByKey.contains(key)) {
            return false;
        }
        MaxLength = Max(MaxLength, static_cast<ui64>(range.Size()));
        auto [it, inserted] = Ranges.emplace(
            TItem{.Key = key, .Range = range, .Value = std::move(value)});
        Y_DEBUG_ABORT_UNLESS(inserted);
        RangeByKey[key] = it;
        return true;
    }

    // Removes block range specified by Key from the collection. Returns
    // extracted range and it value.
    [[nodiscard]] std::optional<TItem> ExtractRange(TKey key)
    {
        auto it = RangeByKey.find(key);
        if (it != RangeByKey.end()) {
            auto& item = const_cast<TItem&>(*it->second);
            std::optional<TItem> result(TItem{
                .Key = item.Key,
                .Range = item.Range,
                .Value = std::move(item.Value)});

            Ranges.erase(it->second);
            RangeByKey.erase(it);

            return result;
        }

        return std::nullopt;
    }

    // Find item by Key
    [[nodiscard]] std::optional<TFindItem> GetValue(TKey key)
    {
        auto it = RangeByKey.find(key);
        if (it != RangeByKey.end()) {
            return MakeFindItem(it->second);
        }

        return std::nullopt;
    }

    // Find item by Key
    [[nodiscard]] std::optional<const TFindItem> GetValue(TKey key) const
    {
        auto it = RangeByKey.find(key);
        if (it != RangeByKey.end()) {
            return MakeFindItem(it->second);
        }

        return std::nullopt;
    }

    // Removes block range specified by Key from the collection. Returns false
    // if the range was not found in the collection.
    bool RemoveRange(TKey key)
    {
        return ExtractRange(key).has_value();
    }

    // Checks that the other range overlaps with any range in Ranges.
    // A pointer to the item describing the range will be returned. Otherwise,
    // nullptr will be returned.
    [[nodiscard]] std::optional<TFindItem> FindFirstOverlapping(
        TBlockRange other)
    {
        std::optional<TFindItem> result = std::nullopt;

        EnumerateOverlapping(
            other,
            [&](TFindItem& item)
            {
                result.emplace(item);
                return EEnumerateContinuation::Stop;
            });

        return result;
    }

    // Checks that the other range overlaps with any range in Ranges.
    [[nodiscard]] std::optional<const TFindItem> FindFirstOverlapping(
        TBlockRange other) const
    {
        std::optional<const TFindItem> result = std::nullopt;

        EnumerateOverlapping(
            other,
            [&](const TFindItem& item)
            {
                result.emplace(item);
                return EEnumerateContinuation::Stop;
            });

        return result;
    }

    // Checks that the other range overlaps with any range in Ranges.
    [[nodiscard]] bool HasOverlaps(TBlockRange other) const
    {
        // 1. Find the range x which: x.end >= other.start in the list sorted
        //    by end of range + length + key.
        // 2. Move through the list of ranges. Check overlapping x with other.
        // 3. when x.begin >= other.end + MaxLength stop iterating.

        auto left = TSearchKey{
            .Key = {},
            .Range = TBlockRange::MakeClosedInterval(0, other.Start)};
        const ui64 safeRight = (Max<ui64>() - MaxLength) > other.End
                                   ? other.End + MaxLength
                                   : Max<ui64>();
        for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
            if (it->Range.Overlaps(other)) {
                return true;
            }
            if (safeRight <= it->Range.Start) {
                break;
            }
        }
        return false;
    }

    // Enumerate all overlapped ranges.
    void EnumerateOverlapping(TBlockRange other, TEnumerateFunc f)
    {
        EnumerateOverlappingImpl(other, std::move(f));
    }

    // Enumerate all overlapped ranges without mutable access to items.
    void EnumerateOverlapping(TBlockRange other, TConstEnumerateFunc f) const
    {
        EnumerateOverlappingImpl(other, std::move(f));
    }

    void Enumerate(TEnumerateFunc f)
    {
        EnumerateImpl(std::move(f));
    }

    // Enumerate all ranges without mutable access to items.
    void Enumerate(TConstEnumerateFunc f) const
    {
        EnumerateImpl(std::move(f));
    }

    [[nodiscard]] bool Empty() const
    {
        return Ranges.empty();
    }

    [[nodiscard]] std::optional<TKey> GetMinKey() const
    {
        std::optional<TKey> minKey;
        for (const auto& item: Ranges) {
            if (!minKey || item.Key < *minKey) {
                minKey = item.Key;
            }
        }
        return minKey;
    }

    [[nodiscard]] size_t Size() const
    {
        return Ranges.size();
    }

    void Trim()
    {
        TRanges ranges(
            typename TRanges::allocator_type(Ranges.get_allocator()));
        Ranges.swap(ranges);

        TRangeByKey rangeByKey(
            typename TRangeByKey::allocator_type(RangeByKey.get_allocator()));
        RangeByKey.swap(rangeByKey);
    }

    [[nodiscard]] THashSet<TKey> GetAllKeys() const
    {
        THashSet<TKey> keys;
        for (const auto& [key, _]: RangeByKey) {
            keys.insert(key);
        }
        return keys;
    }

    // Returns a string representation of all ranges in the collection for
    // debugging purposes.
    [[nodiscard]] TString DebugPrint() const
    {
        TStringStream ss;

        for (const auto& item: Ranges) {
            ss << item.Key << item.Range.Print();
        }
        return ss.Str();
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
