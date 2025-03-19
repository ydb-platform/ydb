#pragma once

#include <util/system/types.h>

class TSpillingBlock {

public:
    void* ExternalMemory;
    ui64 BlockSize;
    ui64 Data;
};

class TSpilling {

public:
    TSpilling(ui64 chunkSize) : ChunkSize(chunkSize) {

    }

    TSpillingBlock Empty(ui64 data);
    TSpillingBlock Save(void* buffer, ui64 size, ui64 data);
    TSpillingBlock Append(TSpillingBlock block, void* buffer, ui64 size);
    void Load(TSpillingBlock block, ui64 offset, void* buffer, ui64 size);
    void Delete(TSpillingBlock block);

    ui64 ChunkSize;
    ui64 WriteChunkCount = 0;
    ui64 ReadChunkCount = 0;
    ui64 CurrentChunkCount = 0;
    ui64 MaxChunkCount = 0;
};
