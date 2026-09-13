#include "block_range_field_bitmask.h"

#include <util/generic/bitmap.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

using TBigIntegerType = ui64;

static constexpr size_t BlockPerByte = 8;

TBlockRangeFieldBitMask::TBlockRangeFieldBitMask(
    IArenaAllocatorPtr allocator,
    size_t maxBlockCount)
    : MaxBlockCount(maxBlockCount)
    , Mask(GetMaskSize(), std::move(allocator))
{}

// static
size_t TBlockRangeFieldBitMask::CalcMemoryUsage(size_t blockCount)
{
    Y_ABORT_UNLESS(blockCount && !(blockCount & (blockCount - 1)));
    return blockCount / BlockPerByte;
}

IBlockRangeFieldImpl::EBackend TBlockRangeFieldBitMask::GetBackend() const
{
    return EBackend::Bitmask;
}

bool TBlockRangeFieldBitMask::TryAdd(TBlockRange16 range, bool* changed)
{
    Y_ASSERT(changed);
    *changed = false;

    Y_ABORT_UNLESS(range.End < MaxBlockCount);

    const size_t end =
        Min(static_cast<size_t>(range.End + 1),
            static_cast<size_t>(MaxBlockCount));
    size_t newBits = 0;

    // Count new bits in each chunk, then set them.
    const size_t startChunk = range.Start / BlockPerByte;
    const size_t startBit = range.Start % BlockPerByte;
    const size_t endChunk = end / BlockPerByte;
    const size_t endBit = end % BlockPerByte;

    size_t bitOffset = startBit;
    for (size_t chunk = startChunk; chunk <= endChunk; ++chunk) {
        ui8 chunkMask = static_cast<ui8>(0xFF << bitOffset);
        if (chunk == endChunk) {
            ui8 endMask = static_cast<ui8>(0xFF << endBit);
            chunkMask &= ~endMask;
            if (!chunkMask) {
                break;
            }
        }
        newBits += ::NBitMapPrivate::CountBitsPrivate(~Mask[chunk] & chunkMask);
        Mask[chunk] |= chunkMask;
        bitOffset = 0;
    }

    if (newBits > 0) {
        BlockCount += newBits;
        *changed = true;
    }
    return true;
}

bool TBlockRangeFieldBitMask::TryRemove(TBlockRange16 range, bool* changed)
{
    Y_ASSERT(changed);
    *changed = false;

    Y_ABORT_UNLESS(range.End < MaxBlockCount);
    Y_ABORT_UNLESS(range.Start <= range.End);

    const size_t end =
        Min(static_cast<size_t>(range.End + 1),
            static_cast<size_t>(MaxBlockCount));
    size_t clearedBits = 0;

    // Count cleared bits in each chunk, then reset them.
    const size_t startChunk = range.Start / BlockPerByte;
    const size_t startBit = range.Start % BlockPerByte;
    const size_t endChunk = end / BlockPerByte;
    const size_t endBit = end % BlockPerByte;

    size_t bitOffset = startBit;
    for (size_t chunk = startChunk; chunk <= endChunk; ++chunk) {
        ui8 chunkMask = static_cast<ui8>(0xFF << bitOffset);
        if (chunk == endChunk) {
            ui8 endMask = static_cast<ui8>(0xFF << endBit);
            chunkMask &= ~endMask;
            if (!chunkMask) {
                break;
            }
        }
        clearedBits +=
            ::NBitMapPrivate::CountBitsPrivate(Mask[chunk] & chunkMask);
        Mask[chunk] &= ~chunkMask;
        bitOffset = 0;
    }

    if (clearedBits > 0) {
        BlockCount -= clearedBits;
        *changed = true;
    }
    return true;
}

void TBlockRangeFieldBitMask::Clear()
{
    memset(Mask.GetRawData(), 0, GetMaskSize());
    BlockCount = 0;
}

bool TBlockRangeFieldBitMask::Overlaps(TBlockRange16 other) const
{
    Y_ABORT_UNLESS(other.End < MaxBlockCount);
    return HasAnyInRange(other.Start, other.End + 1);
}

bool TBlockRangeFieldBitMask::Empty() const
{
    return BlockCount == 0;
}

size_t TBlockRangeFieldBitMask::GetBlockCount() const
{
    return BlockCount;
}

TArenaPoolStats TBlockRangeFieldBitMask::GetMemoryStats() const
{
    return {
        .ReservedSize = GetMaskSize(),
        .UsedSize = GetMaskSize(),
        .AllocationCount = 1,
    };
}

std::optional<TBlockRange16> TBlockRangeFieldBitMask::GetFirstRange() const
{
    if (Empty()) {
        return std::nullopt;
    }

    const auto* mask =
        reinterpret_cast<const TBigIntegerType*>(Mask.GetRawData());
    const size_t chunkCount = GetMaskSize() / sizeof(TBigIntegerType);
    constexpr size_t BitsPerChunk = sizeof(TBigIntegerType) * 8;

    // Find the first set bit.
    std::optional<ui64> start;
    for (size_t i = 0; i < chunkCount; ++i) {
        if (mask[i]) {
            start = i * BitsPerChunk + GetValueBitCount(mask[i] & -mask[i]) - 1;
            break;
        }
    }
    if (!start) {
        return std::nullopt;
    }

    // Find the first zero bit at or after *start: the end of the
    // contiguous run of set bits.
    ui64 end = *start;
    const size_t startChunk = *start / BitsPerChunk;
    const size_t startBit = *start % BitsPerChunk;
    for (size_t i = startChunk; i < chunkCount; ++i) {
        TBigIntegerType zeros = ~mask[i];
        if (i == startChunk) {
            // Ignore zero bits below *start within the same chunk.
            zeros &=
                ~((startBit == BitsPerChunk - 1)
                      ? Max<TBigIntegerType>()
                      : ((TBigIntegerType{1} << (startBit + 1)) - 1));
        }
        if (!zeros) {
            end = (i + 1) * BitsPerChunk - 1;
            continue;
        }
        const auto firstZeroBit = GetValueBitCount(zeros & -zeros) - 1;
        if (i * BitsPerChunk + firstZeroBit > end) {
            end = i * BitsPerChunk + firstZeroBit - 1;
        }
        break;
    }

    return TBlockRange16::MakeClosedInterval(
        static_cast<ui16>(*start),
        static_cast<ui16>(end));
}

TString TBlockRangeFieldBitMask::Save() const
{
    return TString{
        reinterpret_cast<const char*>(Mask.GetRawData()),
        GetMaskSize()};
}

TString TBlockRangeFieldBitMask::Print() const
{
    return TStringBuilder() << "Blocks:" << GetBlockCount();
}

void TBlockRangeFieldBitMask::DeserializeFromBitmap(const TString& input)
{
    const size_t count = Min(GetMaskSize(), input.size());
    memcpy(Mask.GetRawData(), input.data(), count);
    for (size_t i = count; i < GetMaskSize(); ++i) {
        Mask[i] = 0;
    }

    RecountBlockCount();
}

void TBlockRangeFieldBitMask::Add(const TBlockRangeFieldBitMask& other)
{
    Y_DEBUG_ABORT_UNLESS(MaxBlockCount == other.MaxBlockCount);

    auto* mask = reinterpret_cast<TBigIntegerType*>(Mask.GetRawData());
    const auto* otherMask =
        reinterpret_cast<const TBigIntegerType*>(other.Mask.GetRawData());
    const size_t count =
        Min(GetMaskSize(), other.GetMaskSize()) / sizeof(TBigIntegerType);

    for (size_t i = 0; i < count; ++i) {
        mask[i] |= otherMask[i];
    }

    RecountBlockCount();
}

void TBlockRangeFieldBitMask::Remove(const TBlockRangeFieldBitMask& other)
{
    Y_DEBUG_ABORT_UNLESS(MaxBlockCount == other.MaxBlockCount);

    auto* mask = reinterpret_cast<TBigIntegerType*>(Mask.GetRawData());
    const auto* otherMask =
        reinterpret_cast<const TBigIntegerType*>(other.Mask.GetRawData());
    const size_t count =
        Min(GetMaskSize(), other.GetMaskSize()) / sizeof(TBigIntegerType);

    for (size_t i = 0; i < count; ++i) {
        mask[i] &= ~otherMask[i];
    }

    RecountBlockCount();
}

bool TBlockRangeFieldBitMask::OverlapsWithBitMask(
    const TBlockRangeFieldBitMask& other) const
{
    Y_DEBUG_ABORT_UNLESS(MaxBlockCount == other.MaxBlockCount);

    const auto* mask =
        reinterpret_cast<const TBigIntegerType*>(Mask.GetRawData());
    const auto* otherMask =
        reinterpret_cast<const TBigIntegerType*>(other.Mask.GetRawData());
    const size_t count =
        Min(GetMaskSize(), other.GetMaskSize()) / sizeof(TBigIntegerType);

    for (size_t i = 0; i < count; ++i) {
        if (mask[i] & otherMask[i]) {
            return true;
        }
    }

    return false;
}

size_t TBlockRangeFieldBitMask::GetMaskSize() const
{
    return CalcMemoryUsage(MaxBlockCount);
}

void TBlockRangeFieldBitMask::RecountBlockCount()
{
    BlockCount = 0;
    const auto* maskPtr =
        reinterpret_cast<const TBigIntegerType*>(Mask.GetRawData());
    for (size_t i = 0; i < GetMaskSize() / sizeof(TBigIntegerType); ++i) {
        BlockCount += ::NBitMapPrivate::CountBitsPrivate(maskPtr[i]);
    }
}

// Check if any bit is set in [start, end), similar to TBitMapOps::HasAny.
bool TBlockRangeFieldBitMask::HasAnyInRange(size_t start, size_t end) const
{
    const size_t startChunk = start / BlockPerByte;
    const size_t startBit = start % BlockPerByte;

    const size_t endChunk = end / BlockPerByte;
    const size_t endBit = end % BlockPerByte;

    size_t bitOffset = startBit;
    for (size_t chunk = startChunk; chunk <= endChunk; ++chunk) {
        ui8 mask = static_cast<ui8>(0xFF << bitOffset);
        if (chunk == endChunk) {
            ui8 endMask = static_cast<ui8>(0xFF << endBit);
            mask &= ~endMask;
            if (!mask) {
                break;
            }
        }
        if (Mask[chunk] & mask) {
            return true;
        }
        bitOffset = 0;
    }
    return false;
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
