#pragma once
#include "defs.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/algorithm.h>
#include <util/generic/queue.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Free chunk holder.
// Part of the in-memory state.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TFreeChunks {
protected:
    TDeque<TChunkIdx> FreeChunks; // TODO(cthulhu): preallocate and use a vector here to reduce allocation count.
    TAtomic FreeChunkCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr MonFreeChunks;
    ui64 OutOfOrderCount;
    const ui64 SortFreeChunksPerItems;
public:
    TFreeChunks(::NMonitoring::TDynamicCounters::TCounterPtr &monFreeChunks, ui64 sortFreeChunksPerItems)
        : FreeChunkCount(0)
        , MonFreeChunks(monFreeChunks)
        , OutOfOrderCount(0)
        , SortFreeChunksPerItems(sortFreeChunksPerItems)
    {}

    void Push(TChunkIdx idx) {
        FreeChunks.push_back(idx);
        AtomicIncrement(FreeChunkCount);
        MonFreeChunks->Inc();
        ++OutOfOrderCount;
    }

    TChunkIdx Pop() {
        if (FreeChunks.empty()) {
            Y_VERIFY(AtomicGet(FreeChunkCount) == 0);
            return 0;
        }
        if (OutOfOrderCount > SortFreeChunksPerItems) {
            Sort(FreeChunks.begin(), FreeChunks.end());
            OutOfOrderCount = 0;
        }
        TChunkIdx idx = FreeChunks.front();
        FreeChunks.pop_front();
        Y_VERIFY(AtomicGet(FreeChunkCount) > 0);
        AtomicDecrement(FreeChunkCount);
        MonFreeChunks->Dec();
        return idx;
    }

    TDeque<TChunkIdx>::const_iterator begin() const {
        return FreeChunks.begin();
    }

    TDeque<TChunkIdx>::const_iterator end() const {
        return FreeChunks.end();
    }

    TChunkIdx PopAt(TDeque<TChunkIdx>::const_iterator it) {
        Y_VERIFY(it != FreeChunks.end());
        Y_VERIFY(FreeChunks.size() > 0);
        TChunkIdx idx = *it;
        FreeChunks.erase(it);
        AtomicDecrement(FreeChunkCount);
        MonFreeChunks->Dec();
        return idx;
    }

    void PushFront(TChunkIdx idx) {
        FreeChunks.push_front(idx);
        AtomicIncrement(FreeChunkCount);
        MonFreeChunks->Inc();
    }

    // A thread-safe function that returns the current number of free chunks.
    ui32 Size() const { return AtomicGet(FreeChunkCount); }
};

} // NPDisk
} // NKikimr
