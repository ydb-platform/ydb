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

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
extern "C" void* UdfAllocateWithSize(ui64 size);
extern "C" void UdfFreeWithSize(const void* mem, ui64 size);
extern "C" [[deprecated("Use UdfAllocateWithSize() instead")]] void* UdfAllocate(ui64 size);
extern "C" [[deprecated("Use UdfFreeWithSize() instead")]] void UdfFree(const void* mem);
#else
extern "C" void* UdfAllocate(ui64 size);
extern "C" void UdfFree(const void* mem);
#endif

namespace NYql {
namespace NUdf {

template <typename Type>
struct TStdAllocatorForUdf
{
    typedef Type value_type;
    typedef Type* pointer;
    typedef const Type* const_pointer;
    typedef Type& reference;
    typedef const Type& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    TStdAllocatorForUdf() noexcept = default;
    ~TStdAllocatorForUdf() noexcept = default;

    template<typename U> TStdAllocatorForUdf(const TStdAllocatorForUdf<U>&) noexcept {};
    template<typename U> struct rebind { typedef TStdAllocatorForUdf<U> other; };
    template<typename U> bool operator==(const TStdAllocatorForUdf<U>&) const { return true; };
    template<typename U> bool operator!=(const TStdAllocatorForUdf<U>&) const { return false; }

    static pointer allocate(size_type n, const void* = nullptr)
    {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
        return static_cast<pointer>(UdfAllocateWithSize(n * sizeof(value_type)));
#else
        return static_cast<pointer>(UdfAllocate(n * sizeof(value_type)));
#endif
    }

    static void deallocate(const_pointer p, size_type n) noexcept
    {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
        (void)(n);
        return UdfFreeWithSize(static_cast<const void*>(p), n * sizeof(value_type));
#else
        (void)(n);
        return UdfFree(static_cast<const void*>(p));
#endif
    }
};

struct TWithUdfAllocator {
    void* operator new(size_t sz) {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
        return UdfAllocateWithSize(sz);
#else
        return UdfAllocate(sz);
#endif
    }

    void* operator new[](size_t sz) {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
        return UdfAllocateWithSize(sz);
#else
        return UdfAllocate(sz);
#endif
    }

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
    void operator delete(void *mem, std::size_t sz) noexcept {
        return UdfFreeWithSize(mem, sz);
    }

    void operator delete[](void *mem, std::size_t sz) noexcept {
        return UdfFreeWithSize(mem, sz);
    }
#else
    void operator delete(void *mem) noexcept {
        return UdfFree(mem);
    }

    void operator delete[](void *mem) noexcept {
        return UdfFree(mem);
    }
#endif
};

} // namespace NUdf
} // namespace NYql

