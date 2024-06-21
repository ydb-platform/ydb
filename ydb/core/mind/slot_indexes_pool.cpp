#include "slot_indexes_pool.h"

namespace NKikimr::NNodeBroker {

static constexpr size_t EXPAND_SIZE = 128;

TSlotIndexesPool::TSlotIndexesPool() {
    FreeIndexes.Flip();
}

void TSlotIndexesPool::Acquire(size_t index) {
    if (index >= FreeIndexes.Size()) {
        size_t oldSize = FreeIndexes.Size();
        FreeIndexes.Reserve(index + 1);
        FreeIndexes.Set(oldSize, FreeIndexes.Size());
    }
    FreeIndexes.Reset(index);
}

size_t TSlotIndexesPool::AcquireLowestFreeIndex() {
    if (FreeIndexes.Empty()) {
        size_t oldSize = FreeIndexes.Size();
        FreeIndexes.Reserve(FreeIndexes.Size() + EXPAND_SIZE);
        FreeIndexes.Set(oldSize, FreeIndexes.Size());
    }
    size_t index = FreeIndexes.FirstNonZeroBit();
    FreeIndexes.Reset(index);
    return index;
}

bool TSlotIndexesPool::IsAcquired(size_t index) const {
    return !FreeIndexes.Test(index);
}

void TSlotIndexesPool::Release(size_t index) {
    if (index < FreeIndexes.Size()) {
        FreeIndexes.Set(index);
    }
}

void TSlotIndexesPool::ReleaseAll() {
    FreeIndexes.Clear();
    FreeIndexes.Flip();
}

size_t TSlotIndexesPool::Capacity() const {
    return FreeIndexes.Size();
}

size_t TSlotIndexesPool::Size() const {
    return FreeIndexes.Size() - FreeIndexes.Count();
}

} // NKikimr::NNodeBroker
