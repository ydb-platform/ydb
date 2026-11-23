#include "block_range_field_flat_set.h"

#include <util/generic/cast.h>

#include <array>
#include <cstring>
#include <optional>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// A single chunk: a count of used ranges plus a sorted array of ranges,
// carved out of 256 bytes.
class TBlockRangeFieldFlatSet::TChunk
{
public:
    static constexpr size_t Capacity = (ChunkSize / sizeof(TRange)) - 1;

    [[nodiscard]] bool Empty() const noexcept
    {
        return Count == 0;
    }

    [[nodiscard]] ui32 GetCount() const noexcept
    {
        return Count;
    }

    [[nodiscard]] bool Full() const noexcept
    {
        return Count == Capacity;
    }

    // First position of a range with Start >= key.
    [[nodiscard]] size_t LowerBound(ui16 key) const
    {
        size_t lo = 0;
        size_t hi = Count;
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            if (Data[mid].Start < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    // Returns the last range. The chunk must not be empty.
    [[nodiscard]] const TRange& Back() const noexcept
    {
        return Data[Count - 1];
    }

    // Removes |count| ranges starting at position |pos|, shifting the
    // tail left.
    void Erase(size_t pos, size_t count)
    {
        for (size_t i = pos; i + count < Count; ++i) {
            Data[i] = Data[i + count];
        }
        Count -= IntegerCast<ui32>(count);
    }

    // Inserts |range| at position |pos|, shifting the tail right.
    // The chunk must not be full.
    void Insert(size_t pos, TRange range)
    {
        Y_DEBUG_ABORT_UNLESS(!Full());
        for (size_t i = Count; i > pos; --i) {
            Data[i] = Data[i - 1];
        }
        Data[pos] = range;
        Count += 1;
    }

    // Appends |range| to the end. The chunk must not be full.
    void PushBack(TRange range)
    {
        Y_DEBUG_ABORT_UNLESS(!Full());
        Data[Count] = range;
        Count += 1;
    }

    // Removes the last range and returns it. The chunk must not be empty.
    TRange PopBack() noexcept
    {
        Y_DEBUG_ABORT_UNLESS(Count > 0);
        Count -= 1;
        return Data[Count];
    }

    // Direct access for the outer class. Use with care.
    [[nodiscard]] const TRange& At(size_t pos) const noexcept
    {
        return Data[pos];
    }

    void SetAt(size_t pos, TRange range) noexcept
    {
        Data[pos] = range;
    }

    void SetCount(ui32 count) noexcept
    {
        Count = count;
    }

    // Adds |range| to the chunk keeping it sorted and merged. Returns
    // std::nullopt when the chunk is full, |canSpill| is false, and the
    // range does not fit without spilling. When |canSpill| is true and
    // the chunk is full, the last range is returned so that the caller
    // can place it into the next chunk.
    // |blockCountDelta| accumulates the change in the number of blocks.
    // |outOfMemory| is set when the range does not fit and cannot be
    // spilled.
    std::optional<TRange> TryAdd(
        TRange range,
        bool canSpill,
        ui32* blockCountDelta,
        bool* outOfMemory)
    {
        const size_t pos = LowerBound(range.Start);

        // Check touching from the left.
        size_t first = pos;
        if (first > 0 &&
            static_cast<ui32>(Data[first - 1].End) + 1 >= range.Start)
        {
            --first;
        }

        // Walk right collecting all ranges that overlap or touch the
        // range.
        ui32 mergedStart = range.Start;
        ui32 mergedEnd = range.End;
        size_t last = first;
        while (last < Count) {
            if (static_cast<ui32>(Data[last].Start) > mergedEnd + 1) {
                break;
            }
            mergedStart = Min<ui32>(mergedStart, Data[last].Start);
            mergedEnd = Max<ui32>(mergedEnd, Data[last].End);
            ++last;
        }

        // Fully covered by a single existing range: nothing changes.
        if (last - first == 1 && Data[first].Start == mergedStart &&
            Data[first].End == mergedEnd)
        {
            return std::nullopt;
        }

        const ui32 erasedBlocks = [first, last, this]
        {
            ui32 blocks = 0;
            for (size_t i = first; i < last; ++i) {
                blocks += Data[i].Size();
            }
            return blocks;
        }();

        const TRange merged = TBlockRange16::MakeClosedInterval(
            IntegerCast<ui16>(mergedStart),
            IntegerCast<ui16>(mergedEnd));

        // Make room at position |first| for the merged range.
        if (first == last) {
            // Insert a new range.
            if (Full()) {
                if (!canSpill) {
                    *outOfMemory = true;
                    return std::nullopt;
                }
                TRange spilled = PopBack();
                Insert(first, merged);
                *blockCountDelta += merged.Size() - erasedBlocks;
                return spilled;
            }
            Insert(first, merged);
        } else if (last - first == 1) {
            // Replace the range in place.
            Data[first] = merged;
        } else {
            // Erase the covered ranges, then write the merged one.
            Erase(first + 1, last - first - 1);
            Data[first] = merged;
        }

        *blockCountDelta += merged.Size() - erasedBlocks;
        return std::nullopt;
    }

    // Removes |range| from the chunk keeping it sorted and merged.
    // Ranges that remain partially outside |range| are trimmed in place.
    // |blockCountDelta| accumulates the change in the number of blocks.
    void TryRemove(TRange range, ui32* blockCountDelta)
    {
        // Find the first range that overlaps |range| (adjacency does not
        // count).
        size_t idx = LowerBound(range.Start);
        if (idx == Count || Data[idx].Start > range.End) {
            if (idx == 0 || Data[idx - 1].End < range.Start) {
                // Nothing overlaps the range.
                return;
            }
            --idx;
        }

        // Walk the run of overlapping ranges computing the remaining
        // tails.
        bool hasLeftTail = false;
        ui32 leftTailStart = 0;
        ui32 leftTailEnd = 0;
        bool hasRightTail = false;
        ui32 rightTailStart = 0;
        ui32 rightTailEnd = 0;

        size_t last = idx;
        ui32 removedBlocks = 0;
        while (last < Count && Data[last].Start <= range.End) {
            const TRange& entry = Data[last];
            removedBlocks += entry.Size();
            if (entry.Start < range.Start) {
                hasLeftTail = true;
                leftTailStart = entry.Start;
                leftTailEnd = range.Start - 1;
            }
            if (entry.End > range.End) {
                hasRightTail = true;
                rightTailStart = range.End + 1;
                rightTailEnd = entry.End;
            }
            ++last;
        }

        *blockCountDelta -= removedBlocks;

        // Replace the first overlapping range with the left tail (if
        // any), then erase the rest of the run.
        if (hasLeftTail) {
            Data[idx] = TBlockRange16::MakeClosedInterval(
                IntegerCast<ui16>(leftTailStart),
                IntegerCast<ui16>(leftTailEnd));
            if (hasRightTail) {
                // The right tail becomes a new range right after the
                // left tail.
                Insert(
                    idx + 1,
                    TBlockRange16::MakeClosedInterval(
                        IntegerCast<ui16>(rightTailStart),
                        IntegerCast<ui16>(rightTailEnd)));
                // Erase the remaining covered ranges between them.
                Erase(idx + 2, last - idx - 1);
            } else {
                Erase(idx + 1, last - idx - 1);
            }
            return;
        }

        if (hasRightTail) {
            Data[idx] = TBlockRange16::MakeClosedInterval(
                IntegerCast<ui16>(rightTailStart),
                IntegerCast<ui16>(rightTailEnd));
            Erase(idx + 1, last - idx - 1);
            return;
        }

        // Fully covered: erase the whole run.
        Erase(idx, last - idx);
    }

private:
    ui32 Count = 0;
    std::array<TRange, Capacity> Data;
};

////////////////////////////////////////////////////////////////////////////////

// TBlockRangeFieldFlatSet

TBlockRangeFieldFlatSet::TBlockRangeFieldFlatSet(
    IArenaAllocatorPtr allocator,
    size_t maxSizeBytes)
    : Allocator(allocator)
    , MaxChunks(maxSizeBytes / ChunkSize)
{}

IBlockRangeFieldImpl::EBackend TBlockRangeFieldFlatSet::GetBackend() const
{
    return EBackend::FlatSet;
}

bool TBlockRangeFieldFlatSet::TryAdd(TRange range, bool* changed)
{
    *changed = false;

    // Find the chunk that should hold the range.
    size_t chunkIndex = FindChunk(range.Start);

    TRange current = range;
    ui32 blockCountDelta = 0;
    bool anyChanged = false;

    while (chunkIndex < Chunks.size()) {
        bool outOfMemory = false;
        std::optional<TRange> spilled = Chunks[chunkIndex]->TryAdd(
            current,
            /*canSpill=*/true,
            &blockCountDelta,
            &outOfMemory);
        if (outOfMemory) {
            BlockCount += blockCountDelta;
            return false;
        }
        if (!spilled) {
            // The range was merged in (or fully covered): done.
            BlockCount += blockCountDelta;
            *changed = anyChanged;
            return true;
        }
        anyChanged = true;
        // The spilled range must go into the next chunk.
        current = *spilled;
        ++chunkIndex;
    }

    // The range (or the spilled one) needs a new chunk at the end.
    TChunk* chunk = AllocChunk();
    if (!chunk) {
        BlockCount += blockCountDelta;
        *changed = anyChanged;
        return anyChanged;
    }
    chunk->PushBack(current);
    anyChanged = true;

    BlockCount += blockCountDelta;
    *changed = anyChanged;
    return true;
}

bool TBlockRangeFieldFlatSet::TryRemove(TRange range, bool* changed)
{
    *changed = false;
    if (Chunks.empty()) {
        return false;
    }

    ui32 blockCountDelta = 0;
    bool anyChanged = false;

    // Walk the chunks from the one that should hold range.Start to the
    // one that should hold range.End.
    const ui16 lastKey = range.End;
    size_t chunkIndex = FindChunk(range.Start);
    while (chunkIndex < Chunks.size()) {
        TChunk* chunk = Chunks[chunkIndex];
        if (chunk->Empty() || chunk->Back().Start > lastKey) {
            // This chunk cannot contain overlapping ranges anymore.
            break;
        }
        const ui32 countBefore = chunk->GetCount();
        chunk->TryRemove(range, &blockCountDelta);
        if (chunk->GetCount() != countBefore) {
            anyChanged = true;
            if (chunk->Empty()) {
                // FixupChunks will release it; advance past it.
                ++chunkIndex;
                continue;
            }
        }
        if (chunk->Back().Start > lastKey) {
            break;
        }
        ++chunkIndex;
    }

    if (anyChanged) {
        BlockCount += blockCountDelta;
        // Recompute RangeCount from chunk counters.
        ui32 total = 0;
        for (const TChunk* chunk: Chunks) {
            total += chunk->GetCount();
        }
        RangeCount = total;
        FixupChunks();
        *changed = true;
    }
    return anyChanged;
}

void TBlockRangeFieldFlatSet::Clear()
{
    for (TChunk* chunk: Chunks) {
        FreeChunk(chunk);
    }
    Chunks.clear();
    RangeCount = 0;
    BlockCount = 0;
}

////////////////////////////////////////////////////////////////////////////////

bool TBlockRangeFieldFlatSet::Overlaps(TRange other) const
{
    if (Chunks.empty()) {
        return false;
    }

    // The chunk that should hold other.Start is the only candidate: all
    // ranges in previous chunks end before it, all ranges in the
    // following chunks start after it.
    const TChunk* chunk = Chunks[FindChunk(other.Start)];
    if (chunk->Empty()) {
        return false;
    }

    const size_t idx = chunk->LowerBound(other.Start);
    if (idx < chunk->GetCount() && chunk->At(idx).Start <= other.End) {
        return true;
    }
    return idx > 0 && chunk->At(idx - 1).End >= other.Start;
}

void TBlockRangeFieldFlatSet::Enumerate(TEnumerateFunc func) const
{
    for (const TChunk* chunk: Chunks) {
        for (size_t i = 0; i < chunk->GetCount(); ++i) {
            if (func(chunk->At(i)) == EEnumerateContinuation::Stop) {
                return;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TBlockRangeFieldFlatSet::Empty() const
{
    return Chunks.empty();
}

size_t TBlockRangeFieldFlatSet::GetBlockCount() const
{
    return BlockCount;
}

size_t TBlockRangeFieldFlatSet::GetSegmentCount() const
{
    return RangeCount;
}

TArenaPoolStats TBlockRangeFieldFlatSet::GetMemoryStats() const
{
    return {
        .ReservedSize = Chunks.size() * ChunkSize,
        .UsedSize = Chunks.size() * sizeof(ui32) + RangeCount * sizeof(TRange),
        .AllocationCount = 0,
    };
}

////////////////////////////////////////////////////////////////////////////////

// Private helpers

size_t TBlockRangeFieldFlatSet::LowerBound(ui16 key) const
{
    size_t lo = 0;
    size_t hi = RangeCount;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (GetRange(mid).Start < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

TBlockRangeFieldFlatSet::TChunk* TBlockRangeFieldFlatSet::GetChunk(
    size_t position)
{
    return Chunks[position / RangesPerChunk];
}

const TBlockRangeFieldFlatSet::TChunk* TBlockRangeFieldFlatSet::GetChunk(
    size_t position) const
{
    return Chunks[position / RangesPerChunk];
}

const TBlockRange16& TBlockRangeFieldFlatSet::GetRange(size_t position) const
{
    return GetChunk(position)->At(position % RangesPerChunk);
}

void TBlockRangeFieldFlatSet::SetRange(size_t position, TRange range)
{
    GetChunk(position)->SetAt(position % RangesPerChunk, range);
}

void TBlockRangeFieldFlatSet::FixupChunks()
{
    size_t remaining = RangeCount;
    for (TChunk* chunk: Chunks) {
        chunk->SetCount(static_cast<ui32>(Min(remaining, RangesPerChunk)));
        remaining -= chunk->GetCount();
    }
    while (!Chunks.empty() && Chunks.back()->Empty()) {
        FreeChunk(Chunks.back());
        Chunks.pop_back();
    }
}

TBlockRangeFieldFlatSet::TChunk* TBlockRangeFieldFlatSet::AllocChunk()
{
    if (Chunks.size() >= MaxChunks) {
        return nullptr;
    }
    void* ptr = Allocator->Allocate(ChunkSize);
    if (!ptr) {
        return nullptr;
    }
    auto* chunk = static_cast<TChunk*>(ptr);
    chunk->SetCount(0);
    Chunks.push_back(chunk);
    return chunk;
}

void TBlockRangeFieldFlatSet::FreeChunk(TChunk* chunk)
{
    Allocator->DeAllocate(chunk);
}

// Low-level primitives used by TryAdd/TryRemove.

size_t TBlockRangeFieldFlatSet::FindChunk(ui16 key) const
{
    // The first chunk whose last range starts at or after the key.
    size_t lo = 0;
    size_t hi = Chunks.size();
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (Chunks[mid]->Back().Start < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return Min(lo, Chunks.size() - 1);
}

void TBlockRangeFieldFlatSet::EraseRanges(size_t position, size_t count)
{
    const size_t startChunk = position / RangesPerChunk;
    const size_t endPosition = position + count;
    const size_t endChunk =
        Min((endPosition + RangesPerChunk - 1) / RangesPerChunk, Chunks.size());

    // Erase inside fully covered chunks.
    for (size_t i = startChunk; i < endChunk; ++i) {
        TChunk* chunk = Chunks[i];
        const size_t chunkBegin = i * RangesPerChunk;
        const size_t erasePos =
            (position > chunkBegin) ? position - chunkBegin : 0;
        const size_t eraseEnd =
            Min(endPosition - chunkBegin,
                static_cast<size_t>(chunk->GetCount()));
        chunk->Erase(erasePos, eraseEnd - erasePos);
    }

    // Shift the tail left across chunk boundaries.
    const size_t tailSize = RangeCount - endPosition;
    for (size_t i = 0; i < tailSize; ++i) {
        SetRange(position + i, GetRange(endPosition + i));
    }
    RangeCount -= IntegerCast<ui32>(count);
    FixupChunks();
}

bool TBlockRangeFieldFlatSet::InsertRange(size_t position, TRange range)
{
    // Fail fast when there is no free slot at all.
    if (RangeCount == Chunks.size() * RangesPerChunk &&
        Chunks.size() >= MaxChunks)
    {
        return false;
    }

    // Make sure the chunk at position has a free slot.
    const size_t chunkIndex = position / RangesPerChunk;
    if (Chunks[chunkIndex]->Full() && !InsertChunkAfter(chunkIndex)) {
        return false;
    }

    // Shift the tail right by one across chunk boundaries.
    for (size_t p = RangeCount; p > position; --p) {
        SetRange(p, GetRange(p - 1));
    }
    SetRange(position, range);
    RangeCount += 1;
    FixupChunks();
    return true;
}

TBlockRangeFieldFlatSet::TChunk* TBlockRangeFieldFlatSet::InsertChunkAfter(
    size_t chunkIndex)
{
    TChunk* newChunk = AllocChunk();
    if (!newChunk) {
        return nullptr;
    }
    // Move the new chunk pointer into its place.
    for (size_t i = Chunks.size() - 1; i > chunkIndex + 1; --i) {
        Chunks[i] = Chunks[i - 1];
    }
    Chunks[chunkIndex + 1] = newChunk;

    // The last range of each preceding chunk moves to the beginning of
    // the next chunk, so the chunk at chunkIndex gets a free slot.
    for (size_t i = Chunks.size() - 1; i > chunkIndex + 1; --i) {
        TChunk* prev = Chunks[i - 1];
        TChunk* cur = Chunks[i];
        cur->Insert(0, prev->PopBack());
    }
    return newChunk;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
