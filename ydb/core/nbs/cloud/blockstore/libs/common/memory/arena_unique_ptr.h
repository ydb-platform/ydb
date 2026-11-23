#pragma once

#include "arena_allocator.h"

#include <utility>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

template <typename T>
class TArenaArrayUniquePtr
{
public:
    TArenaArrayUniquePtr(size_t size, IArenaAllocatorPtr allocator)
        : Allocator(std::move(allocator))
        , Data(static_cast<T*>(Allocator->Allocate(size * sizeof(T))))
    {}

    ~TArenaArrayUniquePtr()
    {
        if (Data) {
            Allocator->DeAllocate(Data);
        }
    }

    T* GetRawData()
    {
        return Data;
    }

    const T* GetRawData() const
    {
        return Data;
    }

    T& operator[](size_t index)
    {
        return Data[index];
    }

    const T& operator[](size_t index) const
    {
        return Data[index];
    }

private:
    IArenaAllocatorPtr Allocator;
    T* Data = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
}   // namespace NYdb::NBS::NBlockStore
