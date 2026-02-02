#pragma once

#include "public.h"

#include <util/generic/cast.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/generic/ylimits.h>

#include <concepts>
#include <optional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Range [start, end]. End value included to range.
template <std::unsigned_integral TBlockIndex>
struct TBlockRange
{
    struct TDifference
    {
        std::optional<TBlockRange> First;
        std::optional<TBlockRange> Second;
    };

    TBlockIndex Start = 0;
    TBlockIndex End = 0;   // End value included.

    static constexpr TBlockIndex MaxIndex = ::Max<TBlockIndex>();

    TBlockRange() = default;


    // Create range [0, std::numeric_limits<TBlockRange>::max()]
    static TBlockRange Max()
    {
        return {0, MaxIndex};
    }

    // Create a range from a single block.
    static TBlockRange MakeOneBlock(TBlockIndex start)
    {
        return TBlockRange{start, start};
    }

    // Create range [start, includedEnd].
    static TBlockRange MakeClosedInterval(
        TBlockIndex start,
        TBlockIndex includedEnd)
    {
        Y_DEBUG_ABORT_UNLESS(start <= includedEnd);
        return TBlockRange{start, includedEnd};
    }

    // Create range [start, Min(includedEnd, upperLimit)].
    // It is guaranteed that there will be no overflow for the end of the
    // interval. Therefore, the constructed range may be shorter than the
    // requested one.
    static TBlockRange MakeClosedIntervalWithLimit(
        TBlockIndex start,
        ui64 includedEnd,
        ui64 upperLimit)
    {
        ui64 end = Min(includedEnd, upperLimit);
        if (end > MaxIndex) {
            end = MaxIndex;
        }
        return TBlockRange::MakeClosedInterval(
            start,
            static_cast<TBlockIndex>(end));
    }

    // Create a range with start and length. It is guaranteed that there will be
    // no overflow for the end of the interval. Therefore, the constructed range
    // may be shorter than the requested one.
    static TBlockRange WithLength(TBlockIndex start, TBlockIndex count)
    {
        Y_DEBUG_ABORT_UNLESS(count);
        if (start < MaxIndex - (count - 1)) {
            return {start, start + (count - 1)};
        } else {
            return {start, MaxIndex};
        }
    }

    static bool TryParse(TStringBuf s, TBlockRange& range);

    [[nodiscard]] TBlockIndex Size() const
    {
        Y_DEBUG_ABORT_UNLESS(Start <= End);
        return (End - Start) + 1;
    }

    // Checks that blockIndex is contained in this range.
    [[nodiscard]] bool Contains(TBlockIndex blockIndex) const
    {
        return Start <= blockIndex && blockIndex <= End;
    }

    // Checks that the other range is completely contained in this range.
    [[nodiscard]] bool Contains(const TBlockRange& other) const
    {
        return Start <= other.Start && other.End <= End;
    }

    // Checks that the other range overlaps with this range.
    [[nodiscard]] bool Overlaps(const TBlockRange& other) const
    {
        return Start <= other.End && other.Start <= End;
    }

    // Create a new range as the intersection of this range with another. The
    // ranges must overlap.
    [[nodiscard]] TBlockRange Intersect(const TBlockRange& other) const
    {
        Y_DEBUG_ABORT_UNLESS(Overlaps(other));

        auto start = ::Max(Start, other.Start);
        auto end = ::Min(End, other.End);
        return {start, end};
    }

    // Subtract |other| from |*this|
    [[nodiscard]] TDifference Difference(const TBlockRange& other) const
    {
        if (!Overlaps(other)) {
            // Ranges not overlapped, do not cut.
            return {*this, std::nullopt};
        }
        if (Start < other.Start && End > other.End) {
            // cut out from the middle
            return {
                TBlockRange(Start, other.Start - 1),
                TBlockRange(other.End + 1, End)};
        }
        if (Start < other.Start) {
            // cut off on the right
            return {TBlockRange(Start, other.Start - 1), std::nullopt};
        }
        if (End > other.End) {
            // cut off on the left
            return {TBlockRange(other.End + 1, End), std::nullopt};
        }
        // completely clear range
        return {};
    }

    // Create new range as the union of this range with other. The ranges must
    // overlap.
    [[nodiscard]] TBlockRange Union(const TBlockRange& other) const
    {
        Y_DEBUG_ABORT_UNLESS(Overlaps(other));

        auto start = ::Min(Start, other.Start);
        auto end = ::Max(End, other.End);
        return {start, end};
    }

    friend bool operator==(const TBlockRange& lhs, const TBlockRange& rhs)
    {
        return lhs.Start == rhs.Start && lhs.End == rhs.End;
    }

    [[nodiscard]] TString Print() const;

private:
    TBlockRange(TBlockIndex start, TBlockIndex end)
        : Start(start)
        , End(end)
    {}
};

////////////////////////////////////////////////////////////////////////////////

template <std::unsigned_integral TBlockIndex>
TString DescribeRange(const TBlockRange<TBlockIndex>& blockRange);
template <std::unsigned_integral TBlockIndex>
TString DescribeRange(const TVector<TBlockIndex>& blocks);

template <typename TBlockIndex>
constexpr auto xrange(const TBlockRange<TBlockIndex>& range)
{
    Y_ABORT_UNLESS(range.End < Max<TBlockIndex>());
    return ::xrange(range.Start, range.End + 1);
}

template <typename TBlockIndex, typename TBlockIndex2>
constexpr auto xrange(const TBlockRange<TBlockIndex>& range, TBlockIndex2 step)
{
    Y_ABORT_UNLESS(range.End < Max<TBlockIndex>());
    return ::xrange(range.Start, range.End + 1, step);
}

////////////////////////////////////////////////////////////////////////////////

// Builds a vector of ranges block by block. To build, you need to call OnBlack
// with the block index that you want to add.
template <typename TBlockIndex>
class TBlockRangeBuilder
{
private:
    TVector<TBlockRange<TBlockIndex>>& Ranges;

public:
    explicit TBlockRangeBuilder(TVector<TBlockRange<TBlockIndex>>& ranges)
        : Ranges(ranges)
    {}

    void OnBlock(TBlockIndex blockIndex)
    {
        // TODO(drbasic). Handle out-of-order blockIndex.
        // Y_DEBUG_ABORT_UNLESS(Ranges.empty() || Ranges.back().End <= blockIndex);

        if (Ranges.empty()) {
            Ranges.push_back(
                TBlockRange<TBlockIndex>::MakeOneBlock(blockIndex));
        } else {
            auto& lastRange = Ranges.back();
            if (lastRange.End == blockIndex) {
                // nothing to do
            } else if (lastRange.End + 1 == blockIndex) {
                // Enlarge range.
                lastRange.End = blockIndex;
            } else {
                // Make new range.
                Ranges.push_back(
                    TBlockRange<TBlockIndex>::MakeOneBlock(blockIndex));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

using TBlockRange32 = TBlockRange<ui32>;
using TBlockRange64 = TBlockRange<ui64>;
using TBlockRange32Builder = TBlockRangeBuilder<ui32>;
using TBlockRange64Builder = TBlockRangeBuilder<ui64>;

inline TBlockRange32 ConvertRangeSafe(const TBlockRange64& range)
{
    return TBlockRange32::MakeClosedInterval(
        IntegerCast<ui32>(range.Start),
        IntegerCast<ui32>(range.End));
}

inline TBlockRange64 ConvertRangeSafe(const TBlockRange32& range)
{
    return TBlockRange64::MakeClosedInterval(range.Start, range.End);
}

struct TBlockRangeComparator
{
    template <typename TBlockIndex>
    bool operator()(
        const TBlockRange<TBlockIndex>& a,
        const TBlockRange<TBlockIndex>& b) const
    {
        return std::tie(a.Start, a.End) < std::tie(b.Start, b.End);
    }
};

using TBlockRangeSet64 = TSet<TBlockRange64, TBlockRangeComparator>;
using TBlockRangeSet32 = TSet<TBlockRange32, TBlockRangeComparator>;

template <typename T>
IOutputStream& operator<<(IOutputStream& out, const TBlockRange<T>& rhs)
{
    out << rhs.Print();
    return out;
}

}   // namespace NCloud::NBlockStore
