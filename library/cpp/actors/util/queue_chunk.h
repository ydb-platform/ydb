#pragma once

#include "defs.h"

template <typename T, ui32 TSize, typename TDerived>
struct TQueueChunkDerived {
    static const ui32 EntriesCount = (TSize - sizeof(TQueueChunkDerived*)) / sizeof(T);
    static_assert(EntriesCount > 0, "expect EntriesCount > 0");

    volatile T Entries[EntriesCount];
    TDerived* volatile Next;

    TQueueChunkDerived() {
        memset(this, 0, sizeof(TQueueChunkDerived));
    }
};

template <typename T, ui32 TSize>
struct TQueueChunk {
    static const ui32 EntriesCount = (TSize - sizeof(TQueueChunk*)) / sizeof(T);
    static_assert(EntriesCount > 0, "expect EntriesCount > 0");

    volatile T Entries[EntriesCount];
    TQueueChunk* volatile Next;

    TQueueChunk() {
        memset(this, 0, sizeof(TQueueChunk));
    }
};
