#include "range_map.h"

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

TRangeMap::TRangeMap(const TBlockRange64& range)
{
    Blocks.insert(range);
}

TRangeMap::TRangeMap(const TVector<TBlockRange64>& ranges)
{
    for (const auto& r : ranges) {
        Blocks.insert(r);
    }
}

TMaybe<TBlockRange64> TRangeMap::GetBlock(const TBlockRange64& range, bool exact)
{
    if (Blocks.empty()) {
        return Nothing();
    }

    auto it = Blocks.lower_bound(range);

    if (it == Blocks.end()) {
        if (exact) {
            return Nothing();
        } else {
            it = Blocks.begin();
        }
    } else {
        if (!range.Overlaps(*it) && exact) {
            return Nothing();
        }
    }

    auto& r = const_cast<TBlockRange64&>(*it);

    if (range.Overlaps(r)) {
        auto newRange = TBlockRange64::MakeClosedInterval(
            Max(range.Start, r.Start),
            Min(range.End, r.End));

        if (newRange.End != r.End) {
            Blocks.insert(
                TBlockRange64::MakeClosedInterval(newRange.End + 1, r.End));
        }

        if (r.Start < newRange.Start) {
            r.End = newRange.Start - 1;
        } else {
            Blocks.erase(it);
        }
        return newRange;
    } else {
        auto result = r;
        Blocks.erase(it);
        if (range.Size() >= result.Size()) {
            return result;
        } else {
            auto newRange = TBlockRange64::WithLength(
                result.Start,
                Min(result.Size(), range.Size()));
            Blocks.insert(TBlockRange64::MakeClosedInterval(
                newRange.End + 1,
                result.End));
            return newRange;
        }
    }
}

void TRangeMap::PutBlock(const TBlockRange64& r)
{
    auto range = r;
    auto it = Blocks.lower_bound(r);

    Y_ABORT_UNLESS(it == Blocks.end() || !r.Overlaps(*it));

    if (it != Blocks.end() && range.Start == it->End + 1) {
        auto& it_range = const_cast<TBlockRange64&>(*it);
        it_range.End = range.End;
    } else {
        auto result = Blocks.insert(r);
        Y_ABORT_UNLESS(result.second);
        it = result.first;
    }

    auto& it_range = const_cast<TBlockRange64&>(*it);

    if (it != Blocks.begin()) {
        auto prev = std::prev(it);
        auto b = *prev;
        Y_UNUSED(b);
        if (prev->Start == it->End + 1) {
            it_range.End = prev->End;
            Blocks.erase(prev);
        }
    }
}

size_t TRangeMap::Size() const
{
    return Blocks.size();
}

bool TRangeMap::Empty() const
{
    return Size() == 0;
}

TString TRangeMap::DumpRanges() const
{
    TStringStream ss;
    for (const auto& r : Blocks) {
        ss << DescribeRange(r) << " ";
    }
    return ss.Str();
}

}   // namespace NCloud::NBlockStore::NLoadTest
