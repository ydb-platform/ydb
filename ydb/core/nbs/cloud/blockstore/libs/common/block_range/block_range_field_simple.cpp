#include "block_range_field_simple.h"

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool AreConnected(TBlockRange16 left, TBlockRange16 right)
{
    return left.Overlaps(right) ||
           (left.End != TBlockRange16::MaxIndex &&
            left.End + 1 == right.Start) ||
           (right.End != TBlockRange16::MaxIndex &&
            right.End + 1 == left.Start);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockRangeFieldImpl::EBackend TBlockRangeFieldSimple::GetBackend() const
{
    return EBackend::Simple;
}

bool TBlockRangeFieldSimple::TryAdd(TBlockRange16 range, bool* changed)
{
    if (Range && !AreConnected(*Range, range)) {
        return false;
    }

    if (!Range) {
        Range = range;
        *changed = true;
        return true;
    }

    if (Range->Contains(range)) {
        *changed = false;
        return true;
    }

    Range = TBlockRange16::MakeClosedInterval(
        Min(Range->Start, range.Start),
        Max(Range->End, range.End));
    *changed = true;
    return true;
}

bool TBlockRangeFieldSimple::TryRemove(TBlockRange16 range, bool* changed)
{
    if (!Range || !Range->Overlaps(range)) {
        *changed = false;
        return true;
    }

    const auto difference = Range->Difference(range);
    if (difference.Second) {
        return false;
    }

    Range = difference.First;
    *changed = true;
    return true;
}

void TBlockRangeFieldSimple::Clear()
{
    Range.reset();
}

bool TBlockRangeFieldSimple::Overlaps(TBlockRange16 other) const
{
    return Range && Range->Overlaps(other);
}

void TBlockRangeFieldSimple::Enumerate(TEnumerateFunc func) const
{
    if (Range) {
        func(*Range);
    }
}

bool TBlockRangeFieldSimple::Empty() const
{
    return !Range;
}

size_t TBlockRangeFieldSimple::GetBlockCount() const
{
    return Range ? Range->Size() : 0;
}

size_t TBlockRangeFieldSimple::GetSegmentCount() const
{
    return Range ? 1 : 0;
}

size_t TBlockRangeFieldSimple::GetAllocatedSize() const
{
    return 0;
}

size_t TBlockRangeFieldSimple::GetUsedSize() const
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
