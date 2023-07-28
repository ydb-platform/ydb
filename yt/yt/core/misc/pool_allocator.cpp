#include "pool_allocator.h"

#include <util/system/align.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TPoolAllocator::AllocateChunk()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto alignment = Max(
        alignof(TAllocatedBlockHeader),
        alignof(TFreeBlockHeader),
        BlockAlignment_);
    auto alignedHeaderSize = Max(
        AlignUp(sizeof(TAllocatedBlockHeader), alignment),
        AlignUp(sizeof(TFreeBlockHeader), alignment));
    auto alignedBlockSize = AlignUp(BlockSize_, alignment);
    auto fullBlockSize = alignedHeaderSize + alignedBlockSize;

    auto blocksPerChunk = ChunkSize_ < fullBlockSize + alignment ? 1 : (ChunkSize_ - alignment) / fullBlockSize;
    auto chunkSize = blocksPerChunk * fullBlockSize;
    auto chunk = TSharedMutableRef::Allocate(
        chunkSize,
        {.InitializeStorage = false, .ExtendToUsableSize = true},
        Cookie_);
    Chunks_.push_back(chunk);

    auto* current = AlignUp(chunk.Begin(), alignment);
    while (true) {
        auto* blockBegin = current + alignedHeaderSize;
        auto* blockEnd = blockBegin + alignedBlockSize;
        if (blockEnd > chunk.End()) {
            break;
        }

        auto* header = reinterpret_cast<TFreeBlockHeader*>(blockBegin) - 1;
        new(header) TFreeBlockHeader(FirstFree_);
        FirstFree_ = header;

        current += fullBlockSize;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
