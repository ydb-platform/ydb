#include "block_range_field_std_set.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator_pool.h>

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

TBlockRangeFieldStdSet::TBlockRangeFieldStdSet(
    IArenaAllocatorPtr allocator,
    size_t memUsageLimit)
    : MemUsageLimit(memUsageLimit)
    , Pool(std::move(allocator))
    , Intervals(TBlockRangeComparator(), TAllocatorAdapter(&Pool))
{}

IBlockRangeFieldImpl::EBackend TBlockRangeFieldStdSet::GetBackend() const
{
    return EBackend::StdSet;
}

bool TBlockRangeFieldStdSet::TryAdd(TBlockRange16 range, bool* changed)
{
    if (Pool.GetMemoryStats().UsedSize >= MemUsageLimit) {
        return false;
    }

    // Non-overlapping ranges sorted by End are also sorted by Start, so we
    // can iterate forward and stop early.

    // Find first existing interval with End >= range.Start - 1 (adjacent or
    // overlapping on the left side of the new range).
    // When range.Start == 0, "range.Start - 1" would underflow → start from
    // begin() to cover all intervals.
    auto it = (range.Start > 0)
                  ? Intervals.lower_bound(
                        TBlockRange16::MakeClosedInterval(0, range.Start - 1))
                  : Intervals.begin();

    ui16 mergedStart = range.Start;
    ui16 mergedEnd = range.End;
    size_t erasedCount = 0;
    TBlockRange16 firstErased = range;

    while (it != Intervals.end()) {
        // For non-overlapping ranges sorted by End (= sorted by Start), we can
        // stop when the next interval starts strictly after mergedEnd + 1.
        // Guard against overflow when mergedEnd == MaxIndex: in that case every
        // possible Start is <= mergedEnd, so no early exit is possible.
        if (mergedEnd != TBlockRange16::MaxIndex && it->Start > mergedEnd + 1) {
            break;
        }

        if (erasedCount == 0) {
            firstErased = *it;
        }
        mergedStart = Min(mergedStart, it->Start);
        mergedEnd = Max(mergedEnd, it->End);
        it = Intervals.erase(it);
        ++erasedCount;
    }

    const TBlockRange16 merged =
        TBlockRange16::MakeClosedInterval(mergedStart, mergedEnd);
    Intervals.insert(merged);

    *changed = erasedCount != 1 || merged != firstErased;
    return true;
}

bool TBlockRangeFieldStdSet::TryRemove(TBlockRange16 range, bool* changed)
{
    if (Pool.GetMemoryStats().UsedSize >= MemUsageLimit) {
        return false;
    }

    if (Intervals.empty()) {
        *changed = false;
        return true;
    }

    auto it = Intervals.lower_bound(
        TBlockRange16::MakeClosedInterval(0, range.Start));

    bool fieldChanged = false;
    while (it != Intervals.end()) {
        // Since Start is monotonically increasing (non-overlapping + sorted by
        // End), stop once Start is past range.End.
        if (it->Start > range.End) {
            break;
        }

        const TBlockRange16 existing = *it;
        it = Intervals.erase(it);
        fieldChanged = true;

        // Keep the left tail if the existing interval starts before
        // range.Start.
        if (existing.Start < range.Start) {
            Intervals.insert(TBlockRange16::MakeClosedInterval(
                existing.Start,
                range.Start - 1));
        }

        // Keep the right tail if the existing interval ends after range.End.
        if (existing.End > range.End) {
            Intervals.insert(
                TBlockRange16::MakeClosedInterval(range.End + 1, existing.End));
            break;
        }
    }
    *changed = fieldChanged;
    return true;
}

void TBlockRangeFieldStdSet::Clear()
{
    Intervals.clear();
}

bool TBlockRangeFieldStdSet::Overlaps(TBlockRange16 other) const
{
    if (Intervals.empty()) {
        return false;
    }

    auto it = Intervals.lower_bound(
        TBlockRange16::MakeClosedInterval(0, other.Start));

    if (it == Intervals.end()) {
        return false;
    }

    return it->Overlaps(other);
}

void TBlockRangeFieldStdSet::Enumerate(TEnumerateFunc func) const
{
    for (const auto& range: Intervals) {
        if (func(range) == EEnumerateContinuation::Stop) {
            break;
        }
    }
}

bool TBlockRangeFieldStdSet::Empty() const
{
    return Intervals.empty();
}

size_t TBlockRangeFieldStdSet::GetBlockCount() const
{
    size_t total = 0;
    for (const auto& range: Intervals) {
        total += range.Size();
    }
    return IntegerCast<ui16>(total);
}

size_t TBlockRangeFieldStdSet::GetSegmentCount() const
{
    return Intervals.size();
}

TArenaPoolStats TBlockRangeFieldStdSet::GetMemoryStats() const
{
    return Pool.GetMemoryStats();
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
