#pragma once

#include "defs.h"

namespace NKikimr::NNodeBroker {

class TSlotIndexesPool {
public:
    TSlotIndexesPool();

    void Acquire(size_t index);
    size_t AcquireLowestFreeIndex();
    bool IsAcquired(size_t index) const;
    void Release(size_t index);
    void ReleaseAll();
    size_t Capacity() const;
    size_t Size() const;
private:
    TDynBitMap FreeIndexes;
};

} // NKikimr::NNodeBroker
