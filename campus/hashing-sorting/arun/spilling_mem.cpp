#include "spilling_mem.h"

#include <stdlib.h>

#include <algorithm>
#include <cstring>

TSpillingBlock TSpilling::Empty(ui64 data) {
    TSpillingBlock block;

    block.ExternalMemory = nullptr;
    block.BlockSize = 0;
    block.Data = data;

    return block;
}

TSpillingBlock TSpilling::Save(void* buffer, ui64 size, ui64 data) {
    TSpillingBlock block;
    block.ExternalMemory = malloc(size);
    block.BlockSize = size;
    std::memcpy(block.ExternalMemory, buffer, size);
    block.Data = data;

    auto chunkCount = (size + (ChunkSize - 1)) / ChunkSize;
    WriteChunkCount += chunkCount;
    CurrentChunkCount += chunkCount;
    MaxChunkCount = std::max(MaxChunkCount, CurrentChunkCount);

    return block;
}

TSpillingBlock TSpilling::Append(TSpillingBlock currentBlock, void* buffer, ui64 size) {
    TSpillingBlock block;
    auto newSize = currentBlock.BlockSize + size;
    block.ExternalMemory = realloc(currentBlock.ExternalMemory, newSize);
    block.BlockSize = newSize;
    std::memcpy(reinterpret_cast<char *>(block.ExternalMemory) + currentBlock.BlockSize, buffer, size);
    block.Data = currentBlock.Data;

    auto currentChunkCount = (currentBlock.BlockSize + (ChunkSize - 1)) / ChunkSize;
    auto newChunkCount = (newSize + (ChunkSize - 1)) / ChunkSize;
    WriteChunkCount += (size + (ChunkSize - 1)) / ChunkSize;
    CurrentChunkCount += newChunkCount - currentChunkCount;
    MaxChunkCount = std::max(MaxChunkCount, CurrentChunkCount);

    return block;
}

void TSpilling::Load(TSpillingBlock block, ui64 offset, void* buffer, ui64 size) {
    std::memcpy(buffer, reinterpret_cast<char *>(block.ExternalMemory) + offset, size);
    ReadChunkCount += (size + (ChunkSize - 1)) / ChunkSize;
}

void TSpilling::Delete(TSpillingBlock block) {
    free(block.ExternalMemory);
    CurrentChunkCount -= (block.BlockSize + (ChunkSize - 1)) / ChunkSize;
}
