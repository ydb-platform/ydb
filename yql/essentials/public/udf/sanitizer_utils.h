#pragma once

#include <cstddef>

#include <util/system/compiler.h>

#if defined(_asan_enabled_)
    #include <sanitizer/asan_interface.h>
#elif defined(_msan_enabled_)
    #include <sanitizer/msan_interface.h>
#endif

#if defined(_asan_enabled_) || defined(_msan_enabled_)
    #include <util/generic/scope.h>
#endif // defined(_asan_enabled_) || defined(_msan_enabled_)

namespace NYql::NUdf {
namespace NInternal {

constexpr void SanitizerMarkInvalid(void* addr, size_t size) {
    if (addr == nullptr) {
        return;
    }
#if defined(_asan_enabled_)
    __asan_poison_memory_region(addr, size);
#elif defined(_msan_enabled_)
    __msan_poison(addr, size);
#else
    Y_UNUSED(addr, size);
#endif
}
constexpr void SanitizerMarkValid(void* addr, size_t size) {
    if (addr == nullptr) {
        return;
    }
#if defined(_asan_enabled_)
    __asan_unpoison_memory_region(addr, size);
#elif defined(_msan_enabled_)
    __msan_unpoison(addr, size);
#else
    Y_UNUSED(addr, size);
#endif
}

} // namespace NInternal

#if defined(_asan_enabled_)
inline constexpr size_t ALLOCATION_REDZONE_SIZE = 16;
#elif defined(_msan_enabled_)
inline constexpr size_t ALLOCATION_REDZONE_SIZE = 0;
#else
inline constexpr size_t ALLOCATION_REDZONE_SIZE = 0;
#endif
inline constexpr size_t SANITIZER_EXTRA_ALLOCATION_SPACE = ALLOCATION_REDZONE_SIZE * 2;

#if defined(_msan_enabled_)
    #define YQL_MSAN_FREEZE_AND_SCOPED_UNPOISON(addr, size)                     \
        TArrayHolder<char> __yqlMaybePoisonedBuffer(new char[(size)]());        \
        __msan_copy_shadow(__yqlMaybePoisonedBuffer.Get(), (addr), (size));     \
        NYql::NUdf::NInternal::SanitizerMarkValid((addr), (size));              \
        Y_DEFER {                                                               \
            __msan_copy_shadow((addr), __yqlMaybePoisonedBuffer.Get(), (size)); \
        }
#else // defined (_msan_enabled_)
    #define YQL_MSAN_FREEZE_AND_SCOPED_UNPOISON(addr, size)
#endif // defined (_msan_enabled_)

// Mark the memory as deallocated and handed over to the allocator.
// Client -> Allocator.
constexpr void* SanitizerMakeRegionInaccessible(void* addr, size_t size) {
    NInternal::SanitizerMarkInvalid(addr, size);
    return addr;
}

// Mark the memory as allocated and handed over to the client.
// Allocator -> Client.
constexpr void* SanitizerMakeRegionAccessible(void* addr, size_t size) {
#if defined(_asan_enabled_)
    NInternal::SanitizerMarkValid(addr, size);
#elif defined(_msan_enabled_)
    // NOTE: When we give memory from allocator to client we should mark it as invalid.
    NInternal::SanitizerMarkInvalid(addr, size);
#endif
    Y_UNUSED(size);
    return addr;
}

constexpr size_t GetSizeToAlloc(size_t size) {
#if defined(_asan_enabled_) || defined(_msan_enabled_)
    if (size == 0) {
        return 0;
    }
    return size + 2 * ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_) || defined(_msan_enabled_)
    return size;
#endif // defined(_asan_enabled_) || defined(_msan_enabled_)
}

constexpr const void* GetOriginalAllocatedObject(const void* ptr, bool isZeroSize = false) {
#if defined(_asan_enabled_) || defined(_msan_enabled_)
    if (isZeroSize) {
        return ptr;
    }
    return static_cast<const char*>(ptr) - ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_) || defined(_msan_enabled_)
    Y_UNUSED(isZeroSize);
    return ptr;
#endif // defined(_asan_enabled_) || defined(_msan_enabled_)
}

constexpr void* WrapPointerWithRedZones(void* ptr, size_t extendedSizeWithRedzone) {
#if defined(_asan_enabled_) || defined(_msan_enabled_)
    if (extendedSizeWithRedzone == 0) {
        return ptr;
    }
    SanitizerMakeRegionInaccessible(ptr, extendedSizeWithRedzone);
    SanitizerMakeRegionAccessible(static_cast<char*>(ptr) + ALLOCATION_REDZONE_SIZE, extendedSizeWithRedzone - 2 * ALLOCATION_REDZONE_SIZE);
    return static_cast<char*>(ptr) + ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_) || defined(_msan_enabled_)
    Y_UNUSED(extendedSizeWithRedzone);
    return ptr;
#endif // defined(_asan_enabled_) || defined(_msan_enabled_)
}

constexpr const void* UnwrapPointerWithRedZones(const void* ptr, size_t size) {
#if defined(_asan_enabled_) || defined(_msan_enabled_)
    if (size == 0) {
        return ptr;
    }
    SanitizerMakeRegionInaccessible(static_cast<char*>(const_cast<void*>(ptr)) - ALLOCATION_REDZONE_SIZE,
                                    2 * ALLOCATION_REDZONE_SIZE + size);
    return static_cast<const char*>(ptr) - ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_) || defined(_msan_enabled_)
    Y_UNUSED(size);
    return ptr;
#endif // defined(_asan_enabled_) || defined(_msan_enabled_)
}

} // namespace NYql::NUdf
