#pragma once

#include "block_range.h"

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/stream/str.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// TBlockRangeMap is a class that manages a collection of block ranges and it
// key (ui64) with efficient overlap checking capabilities. It's designed to
// store and query block ranges and it key, particularly useful for determining
// if a given range overlaps with any of the stored ranges.
template <typename TKey, typename TValue>
class TBlockRangeMap
{
public:
    struct TItem
    {
        TKey Key = {};
        TBlockRange64 Range;
        TValue Value = {};

        TValue& AccessValue() const
        {
            return const_cast<TValue&>(Value);
        }
    };

    enum class EEnumerateContinuation
    {
        Continue,
        Stop,
    };
    using TEnumerateFunc =
        std::function<EEnumerateContinuation(const TItem& item)>;

private:
    struct TRangeComparator
    {
        bool operator()(const TItem& lhs, const TItem& rhs) const
        {
            return std::tie(lhs.Range.End, lhs.Range.Start, lhs.Key) <
                   std::tie(rhs.Range.End, rhs.Range.Start, rhs.Key);
        }
    };

    ui64 MaxLength = 0;
    TSet<TItem, TRangeComparator> Ranges;
    THashMap<TKey, decltype(Ranges.begin())> RangeByKey;

public:
    // Adds a block range to the collection. Returns false if the key already
    // exists in the collection.
    bool AddRange(TKey key, TBlockRange64 range, TValue value = {})
    {
        if (RangeByKey.contains(key)) {
            return false;
        }
        MaxLength = Max(MaxLength, range.Size());
        auto [it, inserted] = Ranges.insert(
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
            auto node = Ranges.extract(it->second);
            RangeByKey.erase(it);
            TItem& item = node.value();
            return std::make_optional<TItem>(std::move(item));
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
    [[nodiscard]] const TItem* FindFirstOverlapping(TBlockRange64 other) const
    {
        const TItem* result = nullptr;

        EnumerateOverlapping(
            other,
            [&](const TItem& item)
            {
                result = &item;
                return EEnumerateContinuation::Stop;
            });

        return result;
    }

    // Enumerate all overlapped ranges.
    void EnumerateOverlapping(TBlockRange64 other, TEnumerateFunc f) const
    {
        // 1. Find the range x which: x.end >= other.start in the list sorted
        //    by end of range + length + key.
        // 2. Move through the list of ranges. Check overlapping x with other.
        // 3. when x.begin >= other.end + MaxLength stop iterating.

        auto left = TItem{
            .Key = {},
            .Range = TBlockRange64::MakeClosedInterval(0, other.Start)};
        const ui64 safeRight = (Max<ui64>() - MaxLength) > other.End
                                   ? other.End + MaxLength
                                   : Max<ui64>();
        for (auto it = Ranges.lower_bound(left); it != Ranges.end(); ++it) {
            if (it->Range.Overlaps(other)) {
                if (f(*it) == EEnumerateContinuation::Stop) {
                    break;
                }
            }
            if (safeRight <= it->Range.Start) {
                break;
            }
        }
    }

    void Enumerate(TEnumerateFunc f) const
    {
        for (const TItem& item: Ranges) {
            if (f(item) == EEnumerateContinuation::Stop) {
                break;
            }
        }
    }

    [[nodiscard]] bool Empty() const
    {
        return Ranges.empty();
    }

    [[nodiscard]] size_t Size() const
    {
        return Ranges.size();
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

        for (const auto r: Ranges) {
            ss << r.Key << r.Range.Print();
        }
        return ss.Str();
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
