#pragma once

#include "arena_allocator_pool.h"

#include <cstddef>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

template <class T>
class TArenaPoolAdapter
{
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    explicit TArenaPoolAdapter(TArenaAllocatorPool* pool) noexcept
        : Pool(pool)
    {}

    TArenaPoolAdapter(const TArenaPoolAdapter&) noexcept = default;
    TArenaPoolAdapter(TArenaPoolAdapter&&) noexcept = default;
    TArenaPoolAdapter& operator=(const TArenaPoolAdapter&) noexcept = default;
    TArenaPoolAdapter& operator=(TArenaPoolAdapter&&) noexcept = default;

    template <class U>
    TArenaPoolAdapter(const TArenaPoolAdapter<U>& other) noexcept
        : Pool(other.Pool)
    {}

    pointer allocate(size_type n)
    {
        return static_cast<pointer>(Pool->Allocate(n * sizeof(T)));
    }

    void deallocate(pointer ptr, size_type n) noexcept
    {
        Y_UNUSED(n);
        Pool->Deallocate(ptr);
    }

    template <class U>
    struct rebind
    {
        using other = TArenaPoolAdapter<U>;
    };

    [[nodiscard]] TArenaAllocatorPool* GetPool() const noexcept
    {
        return Pool;
    }

private:
    TArenaAllocatorPool* Pool;

    template <class U>
    friend class TArenaPoolAdapter;
};

/////////////////////////////////////////////////////////////////////////////

template <class T1, class T2>
inline bool operator==(
    const TArenaPoolAdapter<T1>& l,
    const TArenaPoolAdapter<T2>& r) noexcept
{
    return l.GetPool() == r.GetPool();
}

template <class T1, class T2>
inline bool operator!=(
    const TArenaPoolAdapter<T1>& l,
    const TArenaPoolAdapter<T2>& r) noexcept
{
    return !(l == r);
}

/////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
