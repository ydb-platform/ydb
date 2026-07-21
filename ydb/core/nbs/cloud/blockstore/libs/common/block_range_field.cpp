#include "block_range_field.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void TBlockRangeField::Add(TBlockRange64 range)
{
    // Non-overlapping ranges sorted by End are also sorted by Start, so we
    // can iterate forward and stop early.

    // Find first existing interval with End >= range.Start - 1 (adjacent or
    // overlapping on the left side of the new range).
    // When range.Start == 0, "range.Start - 1" would underflow → start from
    // begin() to cover all intervals.
    auto it = (range.Start > 0)
                  ? Intervals.lower_bound(
                        TBlockRange64::MakeClosedInterval(0, range.Start - 1))
                  : Intervals.begin();

    ui64 mergedStart = range.Start;
    ui64 mergedEnd = range.End;

    while (it != Intervals.end()) {
        // For non-overlapping ranges sorted by End (= sorted by Start), we can
        // stop when the next interval starts strictly after mergedEnd + 1.
        // Guard against overflow when mergedEnd == MaxIndex: in that case every
        // possible Start is <= mergedEnd, so no early exit is possible.
        if (mergedEnd != TBlockRange64::MaxIndex && it->Start > mergedEnd + 1) {
            break;
        }

        mergedStart = Min(mergedStart, it->Start);
        mergedEnd = Max(mergedEnd, it->End);
        it = Intervals.erase(it);
    }

    Intervals.insert(TBlockRange64::MakeClosedInterval(mergedStart, mergedEnd));
}

void TBlockRangeField::Remove(TBlockRange64 range)
{
    if (Intervals.empty()) {
        return;
    }

    // Find first interval with End >= range.Start (could overlap with range).
    auto it = Intervals.lower_bound(
        TBlockRange64::MakeClosedInterval(0, range.Start));

    while (it != Intervals.end()) {
        // Since Start is monotonically increasing (non-overlapping + sorted by
        // End), stop once Start is past range.End.
        if (it->Start > range.End) {
            break;
        }

        const TBlockRange64 existing = *it;
        it = Intervals.erase(it);

        // Keep the left tail if the existing interval starts before
        // range.Start.
        if (existing.Start < range.Start) {
            Intervals.insert(TBlockRange64::MakeClosedInterval(
                existing.Start,
                range.Start - 1));
        }

        // Keep the right tail if the existing interval ends after range.End.
        if (existing.End > range.End) {
            Intervals.insert(
                TBlockRange64::MakeClosedInterval(range.End + 1, existing.End));
            break;
        }
    }
}

bool TBlockRangeField::Overlaps(TBlockRange64 other) const
{
    if (Intervals.empty()) {
        return false;
    }

    // First interval with End >= other.Start.
    auto it = Intervals.lower_bound(
        TBlockRange64::MakeClosedInterval(0, other.Start));

    if (it == Intervals.end()) {
        return false;
    }

    return it->Overlaps(other);
}

void TBlockRangeField::Enumerate(TEnumerateFunc func) const
{
    for (const auto& range: Intervals) {
        func(range);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
