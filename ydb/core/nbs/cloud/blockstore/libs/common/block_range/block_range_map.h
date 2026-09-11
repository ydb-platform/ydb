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

private:
    friend class TBlockRangeMapAccessor;

    struct TItemKey
    {
        TKey Key;
        TBlockRange Range;

        bool operator<(const TItemKey& other) const
        {
            return std::tie(Range.End, Range.Start, Key) <
                   std::tie(other.Range.End, other.Range.Start, other.Key);
        }
    };

    using TRanges = std::conditional_t<
        UseArenaAllocator,
        TMap<TItemKey, TValue, TLess<TItemKey>, TArenaPoolAdapter<TItemKey>>,
        TMap<TItemKey, TValue, TLess<TItemKey>>>;
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
            TItemKey{.Key = key, .Range = range},
            std::move(value));
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
            auto& rangesIt = it->second;
            std::optional<TItem> result(
                {.Key = rangesIt->first.Key,
                 .Range = rangesIt->first.Range,
                 .Value = std::move(rangesIt->second)});

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
            auto& rangesIt = it->second;
            std::optional<TFindItem> result(
                {.Key = rangesIt->first.Key,
                 .Range = rangesIt->first.Range,
                 .Value = rangesIt->second});

            return result;
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
    [[nodiscard]] bool HasOverlaps(TBlockRange other) const
    {
        // 1. Find the range x which: x.end >= other.start in the list sorted
        //    by end of range + length + key.
        // 2. Move through the list of ranges. Check overlapping x with other.
        // 3. when x.begin >= other.end + MaxLength stop iterating.

        auto left = TItemKey{
            .Key = {},
            .Range = TBlockRange::MakeClosedInterval(0, other.Start)};
        const ui64 safeRight = (Max<ui64>() - MaxLength) > other.End
                                   ? other.End + MaxLength
                                   : Max<ui64>();
        for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
            const auto& itemKey = it->first;
            if (itemKey.Range.Overlaps(other)) {
                return true;
            }
            if (safeRight <= itemKey.Range.Start) {
                break;
            }
        }
        return false;
    }

    // Enumerate all overlapped ranges.
    void EnumerateOverlapping(TBlockRange other, TEnumerateFunc f)
    {
        // 1. Find the range x which: x.end >= other.start in the list sorted
        //    by end of range + length + key.
        // 2. Move through the list of ranges. Check overlapping x with other.
        // 3. when x.begin >= other.end + MaxLength stop iterating.

        auto left = TItemKey{
            .Key = {},
            .Range = TBlockRange::MakeClosedInterval(0, other.Start)};
        const ui64 safeRight = (Max<ui64>() - MaxLength) > other.End
                                   ? other.End + MaxLength
                                   : Max<ui64>();
        for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
            const auto& itemKey = it->first;
            if (itemKey.Range.Overlaps(other)) {
                TFindItem findItem{
                    .Key = itemKey.Key,
                    .Range = itemKey.Range,
                    .Value = it->second};

                if (f(findItem) == EEnumerateContinuation::Stop) {
                    break;
                }
            }
            if (safeRight <= itemKey.Range.Start) {
                break;
            }
        }
    }

    void Enumerate(TEnumerateFunc f)
    {
        for (auto& [itemKey, value]: Ranges) {
            TFindItem findItem{
                .Key = itemKey.Key,
                .Range = itemKey.Range,
                .Value = value};
            if (f(findItem) == EEnumerateContinuation::Stop) {
                break;
            }
        }
    }

    [[nodiscard]] bool Empty() const
    {
        return Ranges.empty();
    }

    [[nodiscard]] std::optional<TKey> GetMinKey() const
    {
        std::optional<TKey> minKey;
        for (const auto& [itemKey, value]: Ranges) {
            if (!minKey || itemKey.Key < *minKey) {
                minKey = itemKey.Key;
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

        for (const auto& [keyAndRange, _]: Ranges) {
            ss << keyAndRange.Key << keyAndRange.Range.Print();
        }
        return ss.Str();
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
