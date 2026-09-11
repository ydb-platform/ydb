#pragma once

#include "public.h"

#include <util/generic/vector.h>

#include <cstddef>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

// Memory usage of one arena slot-size class.
struct TArenaAllocatorStats
{
    size_t SlotSize = 0;
    size_t ArenaSize = 0;
    size_t ReservedSize = 0;
    size_t UsedSize = 0;
    size_t MaxUsedSize = 0;   // Maximum simultaneous usage of one arena
    size_t Count = 0;         // Total allocations performed
};

//////////////////////////////////////////////////////////////////////////////

class IArenaAllocator
{
public:
    virtual ~IArenaAllocator() = default;

    virtual void* Allocate(size_t size) = 0;
    virtual void DeAllocate(void* ptr) = 0;

    [[nodiscard]] virtual size_t AllocatedBlocks() const = 0;
    [[nodiscard]] virtual size_t AllocatedSize() const = 0;
    [[nodiscard]] virtual size_t UsedSize() const = 0;
    [[nodiscard]] virtual TVector<TArenaAllocatorStats> GetStats() const = 0;
};

//////////////////////////////////////////////////////////////////////////////

IArenaAllocatorPtr CreateArenaAllocator();

size_t RoundAllocationSize(size_t size);

//////////////////////////////////////////////////////////////////////////////
}   // namespace NYdb::NBS::NBlockStore
