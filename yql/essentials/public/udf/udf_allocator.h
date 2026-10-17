#pragma once
#include "udf_version.h"
#include <util/system/types.h>
#include <new>
#include <cstddef>
#include <limits>

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
extern "C" void* UdfArrowAllocate(ui64 size);
extern "C" void* UdfArrowReallocate(const void* mem, ui64 prevSize, ui64 size);
extern "C" void UdfArrowFree(const void* mem, ui64 size);
#endif

extern "C" void* UdfAllocateWithSize(ui64 size);
extern "C" void UdfFreeWithSize(const void* mem, ui64 size);

namespace NYql::NUdf {

template <typename Type>
struct TStdAllocatorForUdf {
    using value_type = Type;
    using pointer = Type*;
    using const_pointer = const Type*;
    using reference = Type&;
    using const_reference = const Type&;
    using size_type = size_t;
    using difference_type = ptrdiff_t;

    TStdAllocatorForUdf() noexcept = default;
    ~TStdAllocatorForUdf() noexcept = default;

    template <typename U>
    explicit TStdAllocatorForUdf(const TStdAllocatorForUdf<U>&) noexcept {};
    template <typename U>
    struct rebind { // NOLINT(readability-identifier-naming)
        using other = TStdAllocatorForUdf<U>;
    };
    template <typename U>
    bool operator==(const TStdAllocatorForUdf<U>&) const {
        return true;
    };
    template <typename U>
    bool operator!=(const TStdAllocatorForUdf<U>&) const {
        return false;
    }

    static pointer allocate(size_type n, const void* = nullptr) // NOLINT(readability-identifier-naming)
    {
        return static_cast<pointer>(UdfAllocateWithSize(n * sizeof(value_type)));
    }

    static void deallocate(const_pointer p, size_type n) noexcept // NOLINT(readability-identifier-naming)
    {
        UdfFreeWithSize(static_cast<const void*>(p), n * sizeof(value_type));
    }
};

struct TWithUdfAllocator {
    void* operator new(size_t sz) {
        return UdfAllocateWithSize(sz);
    }

    void* operator new[](size_t sz) {
        return UdfAllocateWithSize(sz);
    }

    void operator delete(void* mem, std::size_t sz) noexcept {
        UdfFreeWithSize(mem, sz);
    }

    void operator delete[](void* mem, std::size_t sz) noexcept {
        UdfFreeWithSize(mem, sz);
    }
};

} // namespace NYql::NUdf
