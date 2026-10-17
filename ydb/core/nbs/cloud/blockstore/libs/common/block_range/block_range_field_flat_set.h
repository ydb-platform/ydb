#pragma once

#include "block_range_field_impl.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// NOTE: This implementation is unfinished. Do not review, validate, or test
// it until the implementation work is complete.
//
// Memory-optimized interval set for block ranges backed by fixed-size
// chunks.
//
// Designed for scenarios with millions of instances:
// - Memory is allocated directly from an IArenaAllocator in 256-byte
//   chunks. The total chunk memory is limited by maxSizeBytes: AllocChunk()
//   fails once MaxChunks chunks are alive.
// - Each chunk (TChunk) stores its own used-range counter plus a sorted
//   array of ranges. When a chunk fills up, the following chunk is used.
// - Chunk pointers are collected in a vector kept sorted by the Start of
//   the first range in the chunk.
// - A middle insertion shifts ranges right across chunk boundaries; the
//   total number of ranges is capped at MaxChunks * RangesPerChunk, so the
//   shift either fits into existing chunks or fails fast.
// - Ranges are kept merged (adjacent/overlapping ranges are coalesced) on
//   insertion and split on removal.
// - If the memory budget is exhausted, TryAdd()/TryRemove() return false
//   (no exception is propagated).
//
// Invariant: all ranges stored in the chunks are pairwise non-overlapping
// and non-adjacent, sorted by Start across chunks and within each chunk.

class TBlockRangeFieldFlatSet
    : public TNodeBasedBlockRangeField
    , public TDisableCopyMove
{
public:
    // Upper bound for the total pool memory, passed to the pool.
    static constexpr size_t DefaultMaxSizeBytes = 4096;
    // Size of a single chunk requested from the arena allocator.
    static constexpr size_t ChunkSize = 256;

    // The allocator is owned and managed by the caller (same as
    // TBlockRangeFieldSet).
    explicit TBlockRangeFieldFlatSet(
        IArenaAllocatorPtr allocator,
        size_t maxSizeBytes = DefaultMaxSizeBytes);

    [[nodiscard]] EBackend GetBackend() const override;

    bool TryAdd(TRange range, bool* changed) override;
    bool TryRemove(TRange range, bool* changed) override;

    void Clear() override;

    [[nodiscard]] bool Overlaps(TRange other) const override;

    void Enumerate(TEnumerateFunc func) const override;

    [[nodiscard]] bool Empty() const override;
    [[nodiscard]] size_t GetBlockCount() const override;
    [[nodiscard]] size_t GetSegmentCount() const override;
    [[nodiscard]] TArenaPoolStats GetMemoryStats() const override;

private:
    // A single chunk: a count of used ranges plus a sorted array of
    // ranges, carved out of 256 bytes. Defined in the .cpp file.
    class TChunk;

    // Hard cap on the number of stored ranges.
    static constexpr size_t RangesPerChunk = (ChunkSize / sizeof(TRange)) - 1;

    // First position of a range with Start >= key (across all chunks).
    [[nodiscard]] size_t LowerBound(ui16 key) const;

    [[nodiscard]] TChunk* GetChunk(size_t position);
    [[nodiscard]] const TChunk* GetChunk(size_t position) const;

    [[nodiscard]] const TRange& GetRange(size_t position) const;
    void SetRange(size_t position, TRange range);

    // Recomputes per-chunk counters from RangeCount and releases chunks
    // that became empty.
    void FixupChunks();

    // Allocates a chunk, returns nullptr when the maxSizeBytes budget is
    // exhausted.
    [[nodiscard]] TChunk* AllocChunk();
    void FreeChunk(TChunk* chunk);

    // Low-level primitives used by TryAdd/TryRemove.

    // Finds the chunk index that should hold a range with the given Start.
    // Returns the last chunk when the key is past all chunks.
    [[nodiscard]] size_t FindChunk(ui16 key) const;

    // Removes |count| ranges starting at flat position |position|. The
    // tail ranges shift left across chunk boundaries. Does not touch
    // RangeCount/BlockCount.
    void EraseRanges(size_t position, size_t count);

    // Inserts |range| at flat position |position|, shifting the tail right
    // across chunk boundaries. Returns false when no free slot exists in
    // any chunk and no new chunk can be allocated. Does not touch
    // RangeCount/BlockCount.
    [[nodiscard]] bool InsertRange(size_t position, TRange range);

    // Allocates a new chunk after the chunk at |chunkIndex| (or at the
    // end when chunkIndex == Chunks.size()) and shifts the tail ranges of
    // the following chunks right by one, so that the chunk at chunkIndex
    // gets one free slot at its end. Returns nullptr when a new chunk
    // cannot be allocated.
    [[nodiscard]] TChunk* InsertChunkAfter(size_t chunkIndex);

    // Pointers to chunks, sorted by the Start of the first range.
    TVector<TChunk*> Chunks;
    std::shared_ptr<IArenaAllocator> Allocator;
    const size_t MaxChunks;
    ui32 RangeCount = 0;
    ui32 BlockCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
